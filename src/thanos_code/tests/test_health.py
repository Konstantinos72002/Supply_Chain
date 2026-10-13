"""Liveness (/health) never calls Databricks; readiness (/ready) reports whether Databricks answers."""
import httpx
import pytest
from fastapi.testclient import TestClient

import supply_chain_api as module

ORIGINAL_RUN_STATEMENT = module.run_statement      # captured before the test fixtures replace it

SUCCEEDED = {'statement_id': 's1', 'status': {'state': 'SUCCEEDED'},
             'manifest': {'schema': {'columns': [{'name': 'ok', 'type_name': 'INT'}]}},
             'result': {'data_array': [['1']]}}


@pytest.fixture
def make_client(monkeypatch):
    """TestClient whose Databricks calls go to `handler`; returns (client, list of Databricks requests)."""
    monkeypatch.setattr(module, 'run_statement', ORIGINAL_RUN_STATEMENT)   # undo the Spark stub of other tests
    opened = []

    def _make(handler):
        calls = []

        def recording(request):
            calls.append(request)
            return handler(request)

        client = TestClient(module.app)
        client.__enter__()
        opened.append(client)
        client.app.state.client = httpx.AsyncClient(transport=httpx.MockTransport(recording),
                                                    base_url='https://example')
        return client, calls

    yield _make
    for client in opened:
        client.__exit__(None, None, None)


def test_health_does_not_call_databricks(make_client):
    client, calls = make_client(lambda request: httpx.Response(500, text='warehouse stopped'))
    r = client.get('/health')
    assert r.status_code == 200 and r.json() == {'status': 'ok'}
    assert calls == []                                           # Databricks was never contacted


def test_ready_when_databricks_answers(make_client):
    client, calls = make_client(lambda request: httpx.Response(200, json=SUCCEEDED))
    r = client.get('/ready')
    assert r.status_code == 200 and r.json() == {'status': 'ready', 'databricks': 'ok'}
    assert len(calls) == 1                                       # one SELECT 1


def test_not_ready_when_databricks_returns_an_error(make_client):
    client, _ = make_client(lambda request: httpx.Response(400, text='TABLE_OR_VIEW_NOT_FOUND gold_fact'))
    r = client.get('/ready')
    assert r.status_code == 503 and r.json() == {'status': 'not_ready', 'databricks': 'unavailable'}
    assert 'gold_fact' not in r.text


def test_not_ready_when_databricks_is_unreachable(make_client):
    def unreachable(request):
        raise httpx.ConnectError('connection refused', request=request)

    client, _ = make_client(unreachable)
    r = client.get('/ready')
    assert r.status_code == 503 and r.json()['status'] == 'not_ready'
