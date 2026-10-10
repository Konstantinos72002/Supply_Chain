"""POST /assistant/query end to end: real API + real Gold data + scripted LLM."""
import pytest
from conftest import H, FakeLLM

from assistant.evaluation import ungrounded_numbers

OTD = [('query_kpi', {'kpi': 'on_time_delivery_rate'})]


def otd_answer(evidence, attempt):
    value = evidence[-1]['result']['results'][0]['value']
    return f"The On-Time Delivery Rate is {value * 100:.2f}% as of {evidence[-1]['result']['as_of_date']}."


def ask(client, api, llm, question='What is our on-time delivery rate?'):
    api.app.state.assistant_llm = llm
    response = client.post('/assistant/query', json={'question': question}, headers=H)
    assert response.status_code == 200, response.text
    return response.json()


def test_kpi_question(client, api):
    r = ask(client, api, FakeLLM(tool_script={'query_kpi': OTD}, composer=otd_answer))
    assert r['status'] == 'answered' and '64.46%' in r['answer']
    assert r['evidence'][0]['result_summary']['results'][0]['value'] == 0.6446
    assert r['as_of_date'] == '2026-10-05'
    assert r['quality'] == {'scores': {k: 90 for k in r['quality']['scores']}, 'passed': True, 'revised': False,
                            'fallback_used': False}
    assert r['trace'] == ['guard:answer', 'supervisor->kpi_analyst', 'kpi_analyst:call query_kpi', 'tool:query_kpi:ok',
                          'kpi_analyst:done', 'supervisor:finish', 'composer', 'checks:pass', 'judge:pass', 'finalize']
    assert not any('FX' in x for x in r['limitations'])          # a ratio: no EUR involved


def test_expedite_question_uses_inventory_tools(client, api):
    def answer(evidence, attempt):
        first = evidence[0]['result']['order_lines'][0]
        return (f"Expedite {first['po_id']} line {first['po_line_number']} first: {first['delayed_qty']} units "
                f"delayed, latest ETA {first['latest_eta_date']}.")
    llm = FakeLLM(specialists=['inventory_analyst'], composer=answer, tool_script={
        'get_delayed_order_lines': [('get_delayed_order_lines', {'only_exposed_products': True, 'limit': 5})]})
    r = ask(client, api, llm, 'Which delayed orders should we expedite first?')
    assert r['status'] == 'answered' and r['quality']['passed']
    assert r['evidence'][0]['result_summary']['count'] == 5 and r['filters']['only_exposed_products'] is True
    assert any('placeholder FX' in x for x in r['limitations'])


def test_two_specialists(client, api):
    def answer(evidence, attempt):
        products = evidence[0]['result']
        worst = evidence[1]['result']['suppliers'][0]
        return (f"{products['count']} CRITICAL products have no active alternate supplier. The supplier with the "
                f"lowest on-time rate is {worst['supplier_id']} at {worst['on_time_delivery_rate'] * 100:.2f}%.")
    llm = FakeLLM(specialists=['inventory_analyst', 'supplier_analyst'], composer=answer, tool_script={
        'get_product_exposure': [('get_product_exposure', {'only_exposed': False, 'criticality': 'CRITICAL',
                                                           'without_active_alternate': True, 'limit': 300})],
        'get_supplier_scorecard': [('get_supplier_scorecard', {})]})
    r = ask(client, api, llm, 'Which critical products lack a backup supplier, and which supplier is least punctual?')
    assert r['status'] == 'answered' and r['answer'].startswith('12 CRITICAL')
    assert [e['tool'] for e in r['evidence']] == ['get_product_exposure', 'get_supplier_scorecard']
    assert 'supervisor->inventory_analyst' in r['trace'] and 'supervisor->supplier_analyst' in r['trace']


def test_hallucinated_number_is_revised(client, api):
    llm = FakeLLM(tool_script={'query_kpi': OTD},
                  composer=lambda ev, n: 'The On-Time Delivery Rate is 64.90%.' if n == 1 else otd_answer(ev, n))
    r = ask(client, api, llm)
    assert r['quality']['revised'] and r['quality']['passed'] and '64.46%' in r['answer']
    assert r['trace'][-6:] == ['composer', 'checks:fail', 'composer:revision', 'checks:pass', 'judge:pass', 'finalize']


def test_persistent_hallucination_falls_back_to_tool_results(client, api):
    r = ask(client, api, FakeLLM(tool_script={'query_kpi': OTD}, composer=lambda ev, n: 'It is 71.3%.'))
    assert r['quality']['fallback_used'] and not r['quality']['passed']
    assert '0.6446' in r['answer'] and '71.3' not in r['answer']
    assert any('quality check failed' in x for x in r['limitations'])


def test_judge_failing_twice_falls_back(client, api):
    llm = FakeLLM(tool_script={'query_kpi': OTD}, composer=otd_answer, judge=lambda n: {'relevance': 40})
    r = ask(client, api, llm)
    assert r['trace'].count('judge:fail') == 2 and 'fallback' in r['trace'] and r['quality']['fallback_used']


def test_judge_error_never_lets_answer_through(client, api):
    llm = FakeLLM(tool_script={'query_kpi': OTD}, composer=otd_answer, judge=lambda n: {'raise': True})
    r = ask(client, api, llm)
    assert r['quality']['fallback_used'] and any(t.startswith('judge:error') for t in r['trace'])


def test_malformed_arguments_are_corrected(client, api):
    llm = FakeLLM(tool_script={'query_kpi': [('query_kpi', {'kpi': 'otd'}), *OTD]}, composer=otd_answer)
    r = ask(client, api, llm)
    assert [e['status'] for e in r['evidence']] == ['error', 'ok']
    assert 'Invalid arguments' in r['evidence'][0]['result_summary']['error']
    assert r['status'] == 'answered' and r['quality']['passed']


def test_api_rule_violation_is_reported_to_the_agent(client, api):
    llm = FakeLLM(tool_script={'query_kpi': [('query_kpi', {'kpi': 'exposed_value', 'group_by': 'month'}), *OTD]},
                  composer=otd_answer)
    r = ask(client, api, llm)
    assert 'snapshot KPI' in r['evidence'][0]['result_summary']['error'] and r['evidence'][1]['status'] == 'ok'


def test_tool_outside_specialist_scope_is_refused(client, api):
    llm = FakeLLM(tool_script={'query_kpi': [('get_product_exposure', {}), *OTD]}, composer=otd_answer)
    r = ask(client, api, llm)
    assert 'not available to you' in r['evidence'][0]['result_summary']['error']


def test_out_of_scope_runs_no_tool(client, api):
    llm = FakeLLM(route='out_of_scope')
    r = ask(client, api, llm, 'Cancel PO-2025-00208 now')
    assert r['status'] == 'out_of_scope' and r['evidence'] == [] and r['trace'] == ['guard:out_of_scope', 'fixed_answer']
    assert llm.calls == ['RouteDecision']


def test_clarification(client, api):
    r = ask(client, api, FakeLLM(route='clarify'), 'How are things?')
    assert r['status'] == 'clarification_needed' and r['answer'] == 'Which KPI do you mean?'


def test_data_unavailable(client, api, monkeypatch):
    from fastapi import HTTPException

    async def down(*args, **kwargs):
        raise HTTPException(status_code=502, detail='warehouse down')
    monkeypatch.setattr(api, 'run_statement', down)
    r = ask(client, api, FakeLLM(tool_script={'query_kpi': OTD}))
    assert r['status'] == 'data_unavailable' and r['trace'][-1] == 'unavailable'


def test_tool_call_limits(client, api):
    endless = [('query_kpi', {'kpi': 'order_value'})] * 20
    llm = FakeLLM(specialists=['kpi_analyst'] * 5, tool_script={'query_kpi': endless},
                  composer=lambda ev, n: 'Order value retrieved.')
    r = ask(client, api, llm)
    assert len(r['evidence']) == 8                                    # MAX_TOOL_CALLS
    assert r['trace'].count('supervisor->kpi_analyst') == 2           # 4 calls per specialist, then total cap
    assert 'supervisor:finish(limit)' in r['trace'] and r['status'] == 'answered'


def test_assistant_requires_sign_in_and_valid_body(client, api):
    assert client.post('/assistant/query', json={'question': 'What is our order value?'}).status_code == 401
    assert client.post('/assistant/query', json={'question': 'x'}, headers=H).status_code == 422
    assert client.post('/assistant/query', json={'question': 'ok?', 'x': 1}, headers=H).status_code == 422


@pytest.mark.parametrize('answer, bad', [
    ('Rate 64.46% (0.6446) as of 2026-10-05 for SUP-0007, PO-2025-00208.', []),
    ('Order value EUR 190.39 million, EUR 190,388,429.60 in total, top 5 lines.', []),
    ('Exposure is EUR 2.57 million across 36 products.', []),
    ('The rate improved by 3.2 points to 71.3%.', ['3.2', '71.3']),
])
def test_number_grounding(answer, bad):
    evidence = [{'data': {'value': 0.6446, 'total': 190388429.6, 'exposed': 2574955.37, 'count': 36}}]
    assert ungrounded_numbers(answer, evidence, 'question') == bad
