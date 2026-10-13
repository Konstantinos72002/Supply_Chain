"""POST /assistant/query end to end: real API + real Gold data + scripted LLM."""
import pytest
from conftest import H, FakeLLM

from assistant.evaluation import ungrounded_numbers

OTD = [('query_kpi', {'kpi': 'on_time_delivery_rate'})]


def otd_answer(evidence, attempt):
    kpi = next(e['result'] for e in reversed(evidence) if e['tool'] == 'query_kpi')
    return f"The On-Time Delivery Rate is {kpi['results'][0]['value'] * 100:.2f}% as of {kpi['as_of_date']}."


def ask(client, api, llm, question='What is our on-time delivery rate?', **body):
    api.app.state.assistant_llm = llm
    response = client.post('/assistant/query', json={'question': question, **body}, headers=H)
    assert response.status_code == 200, response.text
    return response.json()


def test_kpi_question(client, api):
    r = ask(client, api, FakeLLM(tool_script={'query_kpi': OTD}, composer=otd_answer))
    assert r['mode'] == 'extended' and r['status'] == 'answered' and '64.46%' in r['answer']
    assert r['evidence'][0]['result_summary']['results'][0]['value'] == 0.6446
    assert r['as_of_date'] == '2026-10-07'
    assert r['quality'] == {'scores': {k: 90 for k in r['quality']['scores']}, 'passed': True, 'revised': False,
                            'fallback_used': False}
    assert r['trace'] == ['guard:answer', 'supervisor->kpi_analyst', 'kpi_analyst:call query_kpi', 'tool:query_kpi:ok',
                          'kpi_analyst:done', 'supervisor:finish', 'sql_planner:0 queries', 'sql_merge:0/0 ok',
                          'analyst_answer', 'sql_answer:no_data', 'combiner', 'checks:pass', 'judge:pass', 'finalize']
    assert not any('FX' in x for x in r['limitations'])          # a ratio: no EUR involved


def test_simple_mode_skips_the_sql_workers(client, api):
    llm = FakeLLM(tool_script={'query_kpi': OTD}, composer=otd_answer)
    r = ask(client, api, llm, mode='simple')
    assert r['mode'] == 'simple' and r['status'] == 'answered' and '64.46%' in r['answer'] and r['quality']['passed']
    assert r['trace'] == ['guard:answer', 'supervisor->kpi_analyst', 'kpi_analyst:call query_kpi', 'tool:query_kpi:ok',
                          'kpi_analyst:done', 'supervisor:finish', 'simple_answer', 'checks:pass', 'judge:pass',
                          'finalize']
    assert llm.sql_requests == [] and 'SqlPlan' not in llm.calls and 'CombinedDraft' not in llm.calls
    assert not any('SQL' in x for x in r['limitations'])


def test_simple_mode_revises_its_own_answer(client, api):
    llm = FakeLLM(tool_script={'query_kpi': OTD},
                  composer=lambda ev, n: 'The On-Time Delivery Rate is 64.90%.' if n == 1 else otd_answer(ev, n))
    r = ask(client, api, llm, mode='simple')
    assert r['quality']['revised'] and r['quality']['passed'] and '64.46%' in r['answer']
    assert r['trace'][-6:] == ['simple_answer', 'checks:fail', 'simple_answer:revision', 'checks:pass', 'judge:pass',
                               'finalize']


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
    assert r['trace'][-6:] == ['combiner', 'checks:fail', 'combiner:revision', 'checks:pass', 'judge:pass', 'finalize']
    assert r['trace'].count('sql_planner:0 queries') == 1        # a revision rewrites the combined answer only


def test_persistent_hallucination_falls_back_to_tool_results(client, api):
    r = ask(client, api, FakeLLM(tool_script={'query_kpi': OTD}, composer=lambda ev, n: 'It is 71.3%.'))
    assert r['quality']['fallback_used'] and not r['quality']['passed']
    assert '0.6446' in r['answer'] and '71.3' not in r['answer']
    assert any('quality check failed' in x for x in r['limitations'])


def test_judge_failing_twice_falls_back(client, api):
    llm = FakeLLM(tool_script={'query_kpi': OTD}, composer=otd_answer, judge=lambda n: {'clarity': 40})
    r = ask(client, api, llm)
    assert r['trace'].count('judge:fail') == 2 and 'fallback' in r['trace'] and r['quality']['fallback_used']
    assert 'combiner:revision' in r['trace']


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
    assert llm.calls == ['RouteDecision'] and llm.sql_requests == []      # no SQL either


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
    assert client.post('/assistant/query', json={'question': 'ok?', 'mode': 'fast'}, headers=H).status_code == 422


@pytest.mark.parametrize('answer, bad', [
    ('Rate 64.46% (0.6446) as of 2026-10-07 for SUP-0007, PO-2025-00208.', []),
    ('Order value EUR 190.39 million, EUR 190,388,429.60 in total, top 5 lines.', []),
    ('Exposure is EUR 2.57 million across 36 products.', []),
    ('The rate improved by 3.2 points to 71.3%.', ['3.2', '71.3']),
])
def test_number_grounding(answer, bad):
    evidence = [{'data': {'value': 0.6446, 'total': 190388429.6, 'exposed': 2574955.37, 'count': 36}}]
    assert ungrounded_numbers(answer, evidence, 'question') == bad


# --------------------------------------------------------------------------- SQL workers on the Gold tables
BY_REGION = ('Suppliers per region', 'SELECT region, COUNT(*) AS suppliers FROM gold_dim_supplier '
                                     'WHERE supplier_key <> -1 GROUP BY region ORDER BY region')
BY_STATUS = ('Order lines per line status', 'SELECT line_status, COUNT(*) AS lines FROM gold_fact_purchase_order_line '
                                            'GROUP BY line_status ORDER BY line_status')


def sql_answer(evidence, attempt):
    parts = []
    for e in evidence:
        if e['tool'] == 'run_gold_sql':
            parts += [', '.join(str(v) for v in row.values()) for row in e['result']['rows']]
    return 'From the Gold tables: ' + '; '.join(parts) + '.'


def test_sql_workers_run_after_the_analysts_and_both_answers_are_combined(client, api):
    llm = FakeLLM(specialists=['kpi_analyst'], tool_script={'query_kpi': OTD},
                  sql_plan=[BY_REGION, BY_STATUS], composer=sql_answer)
    r = ask(client, api, llm, 'What is our on-time rate, how many suppliers per region and lines per status?')
    assert r['status'] == 'answered' and r['quality']['passed']
    assert [e['tool'] for e in r['evidence']].count('run_gold_sql') == 2 and r['evidence'][0]['tool'] == 'query_kpi'
    t = r['trace']
    assert t[t.index('supervisor:finish'):t.index('sql_planner:2 queries') + 1] == ['supervisor:finish',
                                                                                  'sql_planner:2 queries']
    for name in ('sql_worker[1]:ok', 'sql_worker[2]:ok', 'sql_merge:2/2 ok', 'analyst_answer', 'sql_answer'):
        assert t.index(name) < t.index('combiner')
    combined = llm.combiner_requests[0]
    assert "ANALYSTS' ANSWER (answered):\nThe analysts' answer." in combined
    assert 'SQL ANSWER (answered):\nThe SQL answer.' in combined
    assert 'Findings summarised from the tool results.' in llm.sql_requests[0]   # the planner sees the findings
    sql = next(e for e in r['evidence'] if e['tool'] == 'run_gold_sql')['result_summary']
    assert sql['count'] > 0 and sql['rows'] and f'{api.CATALOG}.{api.SCHEMA}.gold_dim_supplier' in sql['sql']
    assert any('SQL generated by the assistant' in x for x in r['limitations'])
    assert r['as_of_date'] == '2026-10-07'


def test_every_answered_question_passes_the_sql_workers(client, api):
    llm = FakeLLM(tool_script={'query_kpi': OTD}, sql_plan=[BY_REGION], composer=otd_answer)
    r = ask(client, api, llm)
    assert 'sql_planner:1 queries' in r['trace'] and 'sql_worker[1]:ok' in r['trace']
    assert 'MISSING INFORMATION: (none)' in llm.sql_requests[0]
    assert [e['tool'] for e in r['evidence']] == ['query_kpi', 'run_gold_sql'] and '64.46%' in r['answer']


def test_conflicting_answers_are_reported(client, api):
    conflict = 'On-Time Delivery Rate: analysts 64.46%, SQL 63.10%'
    llm = FakeLLM(tool_script={'query_kpi': OTD}, sql_plan=[BY_REGION], composer=otd_answer,
                  conflicts=lambda n: [conflict])
    r = ask(client, api, llm)
    assert 'combiner:1 conflicts' in r['trace'] and r['quality']['passed']
    assert any(x.endswith(conflict) and 'disagreed' in x for x in r['limitations'])


def test_no_analyst_data_is_answered_by_the_sql_workers(client, api):
    llm = FakeLLM(specialists=['supplier_analyst'], sql_plan=[BY_REGION], composer=sql_answer)
    r = ask(client, api, llm, 'How many suppliers do we have per region?')
    assert 'analyst_answer:no_data' in r['trace'] and r['status'] == 'answered'
    assert "ANALYSTS' ANSWER (no_data)" in llm.combiner_requests[0]
    assert [e['tool'] for e in r['evidence']] == ['run_gold_sql']


def test_neither_answer_has_data(client, api):
    r = ask(client, api, FakeLLM(specialists=[]), 'How many suppliers do we have per region?')
    assert r['status'] == 'no_data' and 'combiner:no_data' in r['trace'] and r['trace'][-2:] == ['checks:pass',
                                                                                                 'finalize']


# A product-attribute question the exposure tool cannot answer (it has no product_id filter): the user typed
# PRD-000001, the Gold data stores PRD-00001.
NAME_QUESTION = 'What is the product_name of PRD-000001?'
EXPOSURE = [('get_product_exposure', {})]
PRODUCT_NAME = ('product_name of PRD-000001', "SELECT product_id, product_name FROM gold_dim_product WHERE product_id "
                "LIKE 'PRD-%' AND CAST(regexp_extract(product_id, '([0-9]+)$', 1) AS INT) = 1")


def name_answer(evidence, attempt):
    rows = [row for e in evidence if e['tool'] == 'run_gold_sql' for row in e['result']['rows']]
    return (f"{rows[0]['product_id']} is {rows[0]['product_name']}." if rows
            else 'The product_name of PRD-000001 is not found in the provided evidence.')


def test_supervisor_missing_information_is_passed_to_the_sql_planner(client, api):
    llm = FakeLLM(specialists=['inventory_analyst'], tool_script={'get_product_exposure': EXPOSURE},
                  missing='product_name of PRD-000001', sql_plan=[PRODUCT_NAME], composer=name_answer)
    r = ask(client, api, llm, NAME_QUESTION)
    assert 'MISSING INFORMATION: product_name of PRD-000001' in llm.sql_requests[0]
    assert 'sql_worker[1]:ok' in r['trace']
    assert r['status'] == 'answered' and r['answer'] == 'PRD-00001 is Label Roll C35.' and r['quality']['passed']


def test_empty_tool_result_counts_as_no_data(client, api):
    empty = [('get_product_exposure', {'supplier_id': 'SUP-9999'})]
    llm = FakeLLM(specialists=['inventory_analyst'], tool_script={'get_product_exposure': empty},
                  sql_plan=[PRODUCT_NAME], composer=name_answer)
    r = ask(client, api, llm, NAME_QUESTION)
    assert r['evidence'][0]['result_summary']['count'] == 0
    assert 'analyst_answer:no_data' in r['trace'] and 'Label Roll C35' in r['answer']


def test_sql_that_finds_nothing_is_not_retried(client, api):
    nothing = ('product_name of PRD-999999', "SELECT product_id, product_name FROM gold_dim_product "
                                             "WHERE product_id = 'PRD-999999'")
    llm = FakeLLM(specialists=['inventory_analyst'], tool_script={'get_product_exposure': EXPOSURE},
                  sql_plan=[nothing], composer=name_answer, judge=lambda n: {'completeness': 30} if n == 1 else {})
    r = ask(client, api, llm, NAME_QUESTION)
    assert 'sql_answer:no_data' in r['trace'] and 'combiner:revision' in r['trace']
    assert r['trace'].count('sql_planner:1 queries') == 1
    assert 'not found' in r['answer'] and r['trace'][-1] == 'finalize'


@pytest.mark.parametrize('bad_sql, error', [
    ('SELECT * FROM gold_dim_supplier', 'SELECT * is not allowed'),
    ('SELECT supplier_id, risk_rating FROM gold_dim_supplier', 'confidential'),
    ('SELECT no_such_column FROM gold_dim_supplier', 'The query failed'),
])
def test_sql_worker_fixes_its_query_once(client, api, bad_sql, error):
    llm = FakeLLM(specialists=[], sql_plan=[('Suppliers per region', bad_sql)],
                  sql_fix=lambda failed: BY_REGION[1], composer=sql_answer)
    r = ask(client, api, llm, 'How many suppliers do we have per region?')
    assert [e['status'] for e in r['evidence']] == ['error', 'ok']
    assert error in r['evidence'][0]['result_summary']['error']
    assert r['trace'][r['trace'].index('sql_planner:1 queries') + 1:][:3] == [
        'sql_worker[1]:invalid_arguments', 'sql_worker[1]:fix', 'sql_worker[1]:ok']


def test_sql_warehouse_down_is_data_unavailable(client, api, monkeypatch):
    from fastapi import HTTPException

    async def down(*args, **kwargs):
        raise HTTPException(status_code=502, detail='warehouse down')
    monkeypatch.setattr(api, 'run_statement', down)
    r = ask(client, api, FakeLLM(specialists=[], sql_plan=[BY_REGION]))
    assert r['status'] == 'data_unavailable' and r['trace'][-2:] == ['sql_merge:0/1 ok', 'unavailable']


def test_gold_sql_endpoint(client, api):
    ok = client.post('/analytics/sql', json={'purpose': BY_REGION[0], 'sql': BY_REGION[1]}, headers=H)
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert body['columns'] == ['region', 'suppliers'] and body['count'] == len(body['rows']) > 0
    assert body['truncated'] is False and body['as_of_date'] == '2026-10-07'
    assert f'{api.CATALOG}.{api.SCHEMA}.gold_dim_supplier' in body['sql']

    many = client.post('/analytics/sql', headers=H, json={
        'purpose': 'All lines', 'sql': 'SELECT po_id, po_line_number FROM gold_fact_purchase_order_line'}).json()
    assert many['truncated'] is True and many['count'] == api.SQL_MAX_ROWS

    for sql in ('DELETE FROM gold_dim_product WHERE 1 = 1', 'SELECT 1 AS a; DROP TABLE gold_dim_product',
                'SELECT unit_cost FROM gold_fact_inventory_exposure', 'SELECT shipment_id FROM silver_shipment_events',
                "SELECT path FROM read_files('s3://bucket/x')"):
        response = client.post('/analytics/sql', json={'purpose': 'attack', 'sql': sql}, headers=H)
        assert response.status_code == 400, sql
    assert client.post('/analytics/sql', json={'purpose': 'x y z', 'sql': BY_REGION[1]}).status_code == 401
