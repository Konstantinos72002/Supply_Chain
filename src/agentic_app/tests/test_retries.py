"""Bounded retries towards Databricks: 429/502/503/504 and network errors are retried at most 3 times in total;
other errors (e.g. 400) are not retried. The waits are recorded instead of slept, so the tests run instantly."""
import asyncio

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import app as module

ORIGINAL_RUN_STATEMENT = module.run_statement      # captured before the test fixtures replace it

SUCCEEDED = {'statement_id': 's1', 'status': {'state': 'SUCCEEDED'},
             'manifest': {'schema': {'columns': [{'name': 'ok', 'type_name': 'INT'}]}},
             'result': {'data_array': [['1']]}}


@pytest.fixture
def waits(monkeypatch):
    """Records every retry wait instead of sleeping."""
    recorded = []

    async def fake_sleep(seconds):
        recorded.append(seconds)

    monkeypatch.setattr(module, 'retry_sleep', fake_sleep)
    return recorded


def scripted(*steps):
    """Databricks stand-in that answers with the given steps in order (a Response or an exception to raise)."""
    calls = []

    def handler(request):
        step = steps[len(calls)]
        calls.append(request)
        if isinstance(step, Exception):
            raise step
        return step

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url='https://example')
    return client, calls


def run(client):
    return asyncio.run(ORIGINAL_RUN_STATEMENT(client, 'SELECT 1'))


def test_rate_limit_is_retried_and_succeeds(waits):
    client, calls = scripted(httpx.Response(429), httpx.Response(200, json=SUCCEEDED))
    assert run(client) == [{'ok': 1}]
    assert len(calls) == 2 and waits == [1.0]


def test_backoff_doubles_between_attempts(waits):
    client, calls = scripted(httpx.Response(503), httpx.Response(503), httpx.Response(200, json=SUCCEEDED))
    assert run(client) == [{'ok': 1}]
    assert len(calls) == 3 and waits == [1.0, 2.0]


def test_retry_after_header_is_respected_and_capped(waits):
    client, _ = scripted(httpx.Response(429, headers={'Retry-After': '5'}),
                         httpx.Response(429, headers={'Retry-After': '120'}),
                         httpx.Response(200, json=SUCCEEDED))
    run(client)
    assert waits == [5.0, module.MAX_RETRY_AFTER_SECONDS]


def test_gives_up_after_three_attempts(waits):
    client, calls = scripted(httpx.Response(503), httpx.Response(503), httpx.Response(503))
    with pytest.raises(HTTPException) as exc:
        run(client)
    assert exc.value.status_code == 502
    assert len(calls) == 3 and waits == [1.0, 2.0]                 # bounded: never a 4th call


def test_client_errors_are_not_retried(waits):
    client, calls = scripted(httpx.Response(400, text='bad request'))
    with pytest.raises(HTTPException):
        run(client)
    assert len(calls) == 1 and waits == []


def test_network_errors_are_retried(waits):
    client, calls = scripted(httpx.ConnectError('refused'), httpx.ReadTimeout('slow'),
                             httpx.Response(200, json=SUCCEEDED))
    assert run(client) == [{'ok': 1}]
    assert len(calls) == 3


def test_network_error_after_three_attempts_is_raised(waits):
    client, calls = scripted(httpx.ConnectError('refused'), httpx.ConnectError('refused'),
                             httpx.ConnectError('refused'))
    with pytest.raises(httpx.ConnectError):
        run(client)
    assert len(calls) == 3


def test_polling_requests_are_retried_too(waits):
    pending = {'statement_id': 's1', 'status': {'state': 'PENDING'}}
    client, calls = scripted(httpx.Response(200, json=pending), httpx.Response(503),
                             httpx.Response(200, json=SUCCEEDED))
    assert run(client) == [{'ok': 1}]
    assert [c.method for c in calls] == ['POST', 'GET', 'GET']


def test_endpoint_recovers_from_a_rate_limit(waits, monkeypatch):
    monkeypatch.setattr(module, 'run_statement', ORIGINAL_RUN_STATEMENT)   # undo the Spark stub of other tests
    with TestClient(module.app) as client:
        client.app.state.client, calls = scripted(httpx.Response(429), httpx.Response(200, json=SUCCEEDED))
        r = client.get('/ready')
    assert r.status_code == 200 and r.json()['status'] == 'ready' and len(calls) == 2
