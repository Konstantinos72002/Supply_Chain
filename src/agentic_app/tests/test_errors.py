"""Databricks errors must not leak to the client: generic 502 for the user, full detail in the server log."""
import asyncio
import logging

import httpx
import pytest
from fastapi import HTTPException

import app as module
from conftest import H

ORIGINAL_RUN_STATEMENT = module.run_statement      # captured before the test fixtures replace it
INTERNAL = 'TABLE_OR_VIEW_NOT_FOUND data2026acc123456.team4.gold_fact_purchase_order_line SELECT secret_column'


def mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url='https://example')


def test_http_error_from_databricks_is_hidden(caplog):
    client = mock_client(lambda request: httpx.Response(400, text=INTERNAL))
    with caplog.at_level(logging.ERROR, logger='supply_chain_api'), pytest.raises(HTTPException) as exc:
        asyncio.run(ORIGINAL_RUN_STATEMENT(client, 'SELECT 1'))
    assert exc.value.status_code == 502
    assert 'gold_fact' not in str(exc.value.detail) and 'SELECT' not in str(exc.value.detail)
    assert INTERNAL in caplog.text                                   # the detail is kept in the server log


def test_failed_statement_is_hidden(caplog):
    failed = {'statement_id': 's1', 'status': {'state': 'FAILED', 'error': {'message': INTERNAL}}}
    client = mock_client(lambda request: httpx.Response(200, json=failed))
    with caplog.at_level(logging.ERROR, logger='supply_chain_api'), pytest.raises(HTTPException) as exc:
        asyncio.run(ORIGINAL_RUN_STATEMENT(client, 'SELECT 1'))
    assert exc.value.status_code == 502 and 'gold_fact' not in str(exc.value.detail)
    assert INTERNAL in caplog.text


def test_endpoint_returns_generic_502(monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(module, 'run_statement', ORIGINAL_RUN_STATEMENT)   # undo the Spark stub of other tests
    with TestClient(module.app) as client:
        client.app.state.client = mock_client(lambda request: httpx.Response(400, text=INTERNAL))
        r = client.post('/analytics/query', json={'kpi': 'order_value'}, headers=H)
    assert r.status_code == 502
    assert r.json() == {'detail': 'The data source returned an error. Please try again later.'}
    assert 'gold_fact' not in r.text
