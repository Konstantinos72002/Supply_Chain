"""Correlation id on every request and one structured (JSON) log line per request, without secrets."""
import json
import logging

import pytest
from fastapi.testclient import TestClient

import supply_chain_api as module
from conftest import H


@pytest.fixture
def client():
    with TestClient(module.app) as c:
        yield c


def request_records(caplog):
    return [r for r in caplog.records if r.name == 'supply_chain_api' and r.getMessage() == 'request']


def test_correlation_id_is_generated(client):
    r = client.get('/health')
    cid = r.headers['X-Correlation-ID']
    assert len(cid) == 32 and all(c in '0123456789abcdef' for c in cid)
    assert client.get('/health').headers['X-Correlation-ID'] != cid       # a new id per request


def test_correlation_id_from_client_is_kept(client):
    r = client.get('/health', headers={'X-Correlation-ID': 'demo-123'})
    assert r.headers['X-Correlation-ID'] == 'demo-123'


@pytest.mark.parametrize('bad', ['x' * 65, 'abc def', 'id"}{"level":"FAKE'])
def test_invalid_correlation_id_is_replaced(client, bad):
    r = client.get('/health', headers={'X-Correlation-ID': bad})
    assert r.headers['X-Correlation-ID'] != bad and len(r.headers['X-Correlation-ID']) == 32


def test_one_json_log_line_per_request_without_secrets(client, caplog):
    with caplog.at_level(logging.INFO, logger='supply_chain_api'):
        r = client.get('/health?token=top-secret-value', headers={'X-Correlation-ID': 'demo-456', **H})
    records = request_records(caplog)
    assert len(records) == 1
    line = module.JsonLogFormatter().format(records[0])
    entry = json.loads(line)                                               # valid JSON
    assert entry['correlation_id'] == 'demo-456' == r.headers['X-Correlation-ID']
    assert entry['method'] == 'GET' and entry['path'] == '/health' and entry['status'] == 200
    assert isinstance(entry['duration_ms'], float)
    assert 'top-secret-value' not in line and H['Authorization'][7:] not in line   # no query values, no token


def test_error_requests_are_logged_with_their_status(client, caplog):
    with caplog.at_level(logging.INFO, logger='supply_chain_api'):
        client.post('/analytics/query', json={'kpi': 'order_value'})       # no API key -> 401
    assert json.loads(module.JsonLogFormatter().format(request_records(caplog)[0]))['status'] == 401


def test_assistant_tool_calls_carry_the_same_correlation_id(client, monkeypatch):
    seen = {}

    async def fake_run_assistant(question, tools_client, llm):
        seen['header'] = tools_client.headers.get('X-Correlation-ID')
        return module.AssistantAnswer(status='no_data', answer='-', evidence=[], filters={}, measures=[],
                                      as_of_date=None, limitations=[], quality=None, trace=[])

    monkeypatch.setattr(module, 'run_assistant', fake_run_assistant)
    monkeypatch.setattr(module.app.state, 'assistant_llm', object(), raising=False)
    r = client.post('/assistant/query', json={'question': 'What is our on-time delivery rate?'},
                    headers={'X-Correlation-ID': 'demo-789', **H})
    assert r.status_code == 200
    assert seen['header'] == 'demo-789'                                    # internal tool calls share the id