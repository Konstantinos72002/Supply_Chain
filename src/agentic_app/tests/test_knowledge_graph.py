"""Knowledge graph in the API and the assistant: GET /suppliers/disruption-impact and the network_analyst.

These tests need no Spark: the endpoint reads a small graph file, never the SQL warehouse."""
import os

import pytest
from conftest import H, FakeLLM
from fastapi.testclient import TestClient

import app as module
from knowledge_graph.graph import build_graph, save_graph

SUPPLIERS = [
    {'supplier_id': 'SUP-0017', 'supplier_name': 'Acme', 'is_active': True, 'risk_rating': 'HIGH'},
    {'supplier_id': 'SUP-0023', 'supplier_name': 'Beta', 'is_active': True, 'risk_rating': 'LOW'},
    {'supplier_id': 'SUP-0031', 'supplier_name': 'Gamma', 'is_active': False, 'risk_rating': 'LOW'},
]
PRODUCTS = [
    {'product_id': 'PRD-0042', 'product_name': 'Valve', 'is_critical': True,
     'primary_supplier_id': 'SUP-0017', 'alternate_supplier_id': 'SUP-0023'},
    {'product_id': 'PRD-0117', 'product_name': 'Pump', 'is_critical': True,
     'primary_supplier_id': 'SUP-0017', 'alternate_supplier_id': None},
    {'product_id': 'PRD-0203', 'product_name': 'Bolt', 'is_critical': False,
     'primary_supplier_id': 'SUP-0017', 'alternate_supplier_id': 'SUP-0023'},
]
SHIPMENTS = [{'shipment_id': 'SHP-00881', 'product_id': 'PRD-0117', 'location_id': 'WH-ATH',
              'status_code': 'DELAYED'}]
IMPACT = [('get_supplier_disruption_impact', {'supplier_id': 'SUP-0017'})]


@pytest.fixture
def kg_client(tmp_path, monkeypatch):
    path = save_graph(build_graph(SUPPLIERS, PRODUCTS, SHIPMENTS), tmp_path / 'kg.json')
    monkeypatch.setattr(module, 'KG_PATH', path)
    module._graph_cache.clear()
    with TestClient(module.app) as c:
        yield c
    module.app.state.assistant_llm = None


def ask(client, llm, question):
    module.app.state.assistant_llm = llm
    response = client.post('/assistant/query', json={'question': question}, headers=H)
    assert response.status_code == 200, response.text
    return response.json()


# --------------------------------------------------------------------------- endpoint
def test_disruption_impact(kg_client):
    r = kg_client.get('/suppliers/disruption-impact', params={'supplier_id': 'SUP-0017'}, headers=H)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['supplier_name'] == 'Acme' and body['filters'] == {'supplier_id': 'SUP-0017', 'critical_only': True}
    assert body['affected_product_count'] == 2 and body['uncovered_product_count'] == 1
    pump = body['affected_products'][0]                       # uncovered first
    assert pump['product_id'] == 'PRD-0117' and not pump['has_usable_alternate']
    assert pump['shipments'] == [{'shipment_id': 'SHP-00881', 'status_code': 'DELAYED', 'delivers_to': 'WH-ATH'}]
    assert 'risk_rating' not in r.text                        # confidential, never exposed


def test_disruption_impact_all_products(kg_client):
    r = kg_client.get('/suppliers/disruption-impact', params={'supplier_id': 'SUP-0017', 'critical_only': False},
                      headers=H)
    assert r.json()['affected_product_count'] == 3


def test_disruption_impact_errors(kg_client, tmp_path, monkeypatch):
    url = '/suppliers/disruption-impact'
    assert kg_client.get(url, params={'supplier_id': 'SUP-0017'}).status_code == 401
    assert kg_client.get(url, params={'supplier_id': 'bad id!'}, headers=H).status_code == 422
    assert kg_client.get(url, params={'supplier_id': 'SUP-9999'}, headers=H).status_code == 404
    monkeypatch.setattr(module, 'KG_PATH', tmp_path / 'missing.json')
    assert kg_client.get(url, params={'supplier_id': 'SUP-0017'}, headers=H).status_code == 503


def test_rebuilt_graph_is_reloaded(kg_client):
    url, params = '/suppliers/disruption-impact', {'supplier_id': 'SUP-0017'}
    assert kg_client.get(url, params=params, headers=H).json()['affected_product_count'] == 2
    save_graph(build_graph(SUPPLIERS, PRODUCTS[:1], []), module.KG_PATH)
    stat = module.KG_PATH.stat()
    os.utime(module.KG_PATH, (stat.st_atime, stat.st_mtime + 10))
    assert kg_client.get(url, params=params, headers=H).json()['affected_product_count'] == 1


# --------------------------------------------------------------------------- assistant
def impact_answer(evidence, attempt):
    data = evidence[-1]['result']
    first = data['affected_products'][0]
    return (f"If {data['supplier_id']} stops delivering, {data['affected_product_count']} critical products are hit; "
            f"{first['product_id']} has no active alternate supplier and shipment "
            f"{first['shipments'][0]['shipment_id']} to {first['shipments'][0]['delivers_to']} is in flight.")


def test_what_if_question_uses_knowledge_graph(kg_client):
    llm = FakeLLM(specialists=['network_analyst'], composer=impact_answer,
                  tool_script={'get_supplier_disruption_impact': IMPACT})
    r = ask(kg_client, llm, 'What happens if SUP-0017 stops delivering?')
    assert r['status'] == 'answered' and r['quality']['passed'] and 'PRD-0117' in r['answer']
    assert r['trace'][:5] == ['guard:answer', 'supervisor->network_analyst',
                              'network_analyst:call get_supplier_disruption_impact',
                              'tool:get_supplier_disruption_impact:ok', 'network_analyst:done']
    summary = r['evidence'][0]['result_summary']
    assert summary['uncovered_product_count'] == 1 and summary['supplier_name'] == 'Acme'
    assert r['filters'] == {'supplier_id': 'SUP-0017', 'critical_only': True}
    assert any('knowledge-graph snapshot' in x for x in r['limitations'])
    assert not any('FX' in x for x in r['limitations'])


def test_unknown_supplier_is_reported_to_the_agent(kg_client):
    llm = FakeLLM(specialists=['network_analyst'], composer=impact_answer, tool_script={
        'get_supplier_disruption_impact': [('get_supplier_disruption_impact', {'supplier_id': 'SUP-9999'}), *IMPACT]})
    r = ask(kg_client, llm, 'What if SUP-9999 fails?')
    assert 'not in the knowledge graph' in r['evidence'][0]['result_summary']['error']
    assert r['evidence'][1]['status'] == 'ok' and r['status'] == 'answered'


def test_graph_tool_is_reserved_for_network_analyst(kg_client):
    llm = FakeLLM(specialists=['kpi_analyst'], tool_script={'query_kpi': IMPACT})
    r = ask(kg_client, llm, 'What if SUP-0017 fails?')
    assert 'not available to you' in r['evidence'][0]['result_summary']['error']


def test_hallucination_falls_back_to_graph_results(kg_client):
    llm = FakeLLM(specialists=['network_analyst'], composer=lambda ev, n: '57 products are affected.',
                  tool_script={'get_supplier_disruption_impact': IMPACT})
    r = ask(kg_client, llm, 'What if SUP-0017 fails?')
    assert r['quality']['fallback_used'] and '57' not in r['answer']
    assert 'product_id=PRD-0117' in r['answer'] and 'alternates=none' in r['answer']
    assert 'open shipments=SHP-00881' in r['answer'] and 'alternates=SUP-0023' in r['answer']
