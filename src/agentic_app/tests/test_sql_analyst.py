"""Ad-hoc read-only SQL: POST /analytics/sql (safety rules enforced in code) and the sql_analyst specialist.

Expected values come from the Gold CSV fixtures (tests/data/gold): 17 APAC suppliers, 52 Packaging products,
120 purchase orders with lines ordered in March."""
import re

import pytest
from conftest import H, FakeLLM

import app as module
from assistant import prompts
from assistant.tools import SPECIALIST_TOOLS

APAC = "SELECT COUNT(*) AS supplier_count FROM gold_dim_supplier WHERE region = 'APAC'"
PO_LINES = 'SELECT po_id, po_line_number FROM gold_fact_purchase_order_line ORDER BY po_id, po_line_number'


def sql(client, query, **extra):
    return client.post('/analytics/sql', json={'sql': query, **extra}, headers=H)


def rejected(client, query):
    response = sql(client, query)
    assert response.status_code == 422, response.text
    detail = response.json()['detail']
    assert 'Rewrite the query and retry' in detail              # tells the agent to fix and retry
    return detail


def ask(client, llm, question):
    module.app.state.assistant_llm = llm
    response = client.post('/assistant/query', json={'question': question}, headers=H)
    assert response.status_code == 200, response.text
    return response.json()


# --------------------------------------------------------------------------- endpoint: answers
def test_count_suppliers_in_apac(client):
    body = sql(client, APAC).json()
    assert body['rows'] == [{'supplier_count': 17}] and body['row_count'] == 1 and not body['truncated']
    assert body['tables'] == ['gold_dim_supplier'] and body['columns'] == ['supplier_count']
    assert f'{module.CATALOG}.{module.SCHEMA}.gold_dim_supplier' in body['sql']   # qualified by the API, not the agent


def test_lists_ctes_and_joins(client):
    packaging = sql(client, "SELECT product_id, product_name FROM gold_dim_product "
                            "WHERE category = 'Packaging' ORDER BY product_id").json()
    assert packaging['row_count'] == 52 and packaging['columns'] == ['product_id', 'product_name']
    march = sql(client, 'WITH march AS (SELECT date_key FROM gold_dim_date WHERE month = 3) '
                        'SELECT COUNT(DISTINCT f.po_id) AS purchase_orders FROM gold_fact_purchase_order_line f '
                        'JOIN march m ON f.order_date_key = m.date_key').json()
    assert march['rows'] == [{'purchase_orders': 120}] and march['tables'] == ['gold_dim_date',
                                                                               'gold_fact_purchase_order_line']
    assert sql(client, f'SELECT COUNT(*) AS n FROM {module.CATALOG}.{module.SCHEMA}.gold_dim_location').status_code == 200


def test_sql_requires_a_token(client):
    assert client.post('/analytics/sql', json={'sql': APAC}).status_code == 401


# --------------------------------------------------------------------------- endpoint: safety rules
@pytest.mark.parametrize('query', [
    'DELETE FROM gold_dim_supplier',
    'DROP TABLE gold_dim_supplier',
    'INSERT INTO gold_dim_supplier SELECT supplier_id FROM gold_dim_supplier',
    "UPDATE gold_dim_supplier SET region = 'APAC'",
    'MERGE INTO gold_dim_supplier t USING gold_dim_supplier s ON t.supplier_id = s.supplier_id '
    'WHEN MATCHED THEN DELETE',
    'SELECT supplier_id INTO supplier_copy FROM gold_dim_supplier',
    'SHOW TABLES',
])
def test_only_read_only_select(client, query):
    assert 'read-only SELECT' in rejected(client, query)


def test_only_one_statement(client):
    assert 'exactly one statement' in rejected(client, f'{APAC}; DROP TABLE gold_dim_supplier')


@pytest.mark.parametrize('query', [
    'SELECT supplier_id FROM silver_suppliers',
    'SELECT table_name FROM information_schema.tables',
    'SELECT supplier_id FROM other_catalog.team4.gold_dim_supplier',
    f'SELECT supplier_id FROM {module.CATALOG}.other_schema.gold_dim_supplier',
    'SELECT risk_level_key FROM gold_dim_risk_level',            # Gold, but only holds confidential risk ratings
    "SELECT value FROM read_files('/Volumes/data/landing')",
])
def test_only_gold_tables(client, query):
    assert 'is not allowed. Use only the Gold tables' in rejected(client, query)


@pytest.mark.parametrize('query', [
    'SELECT supplier_id, risk_rating FROM gold_dim_supplier',
    'SELECT AVG(unit_price) AS price FROM gold_fact_purchase_order_line',
    'SELECT product_key FROM gold_fact_inventory_exposure WHERE avg_daily_demand > 10',
    'SELECT supplier_id FROM gold_dim_supplier ORDER BY contract_lead_time_days',
    'SELECT SUM(e.unit_cost) AS cost FROM gold_fact_inventory_exposure e',
    'SELECT po_id, risk_level_key FROM gold_fact_purchase_order_line',
])
def test_no_confidential_columns(client, query):
    assert 'is confidential' in rejected(client, query)


@pytest.mark.parametrize('query', ['SELECT * FROM gold_dim_supplier', 'SELECT s.* FROM gold_dim_supplier s'])
def test_no_select_star(client, query):
    assert 'SELECT * is not allowed' in rejected(client, query)    # would expose confidential columns


def test_no_functions_reaching_outside_gold(client):
    assert 'reflect() is not allowed' in rejected(client, "SELECT reflect('java.lang.System', 'getenv', 'X') AS x")


def test_row_limit_is_always_enforced(client):
    default = sql(client, PO_LINES).json()
    assert default['row_count'] == module.SQL_MAX_ROWS and default['truncated']
    assert default['sql'].endswith(f'LIMIT {module.SQL_MAX_ROWS + 1}')        # one extra row detects truncation
    small = sql(client, PO_LINES, limit=10).json()
    assert small['row_count'] == 10 and small['truncated']
    own = sql(client, PO_LINES + ' LIMIT 5').json()
    assert own['row_count'] == 5 and not own['truncated']
    capped = sql(client, PO_LINES + ' LIMIT 100000').json()
    assert capped['row_count'] == module.SQL_MAX_ROWS and capped['truncated']
    assert sql(client, PO_LINES, limit=module.SQL_MAX_ROWS + 1).status_code == 422
    assert 'whole number' in rejected(client, PO_LINES + ' LIMIT :n')


# --------------------------------------------------------------------------- assistant: sql_analyst
def test_sql_analyst_answers_a_lookup(client):
    llm = FakeLLM(specialists=['sql_analyst'], tool_script={'run_sql_query': [('run_sql_query', {'sql': APAC})]},
                  composer=lambda ev, n: f"There are {ev[0]['result']['rows'][0]['supplier_count']} suppliers in APAC.")
    r = ask(client, llm, 'How many suppliers are in APAC?')
    assert r['status'] == 'answered' and r['answer'] == 'There are 17 suppliers in APAC.'
    assert r['evidence'][0]['result_summary']['rows'] == [{'supplier_count': 17}]
    assert r['trace'][:4] == ['guard:answer', 'supervisor->sql_analyst', 'sql_analyst:call run_sql_query',
                              'tool:run_sql_query:ok']
    assert r['quality']['passed'] and not r['quality']['fallback_used']


def test_rejected_query_goes_back_to_the_agent(client):
    llm = FakeLLM(specialists=['sql_analyst'], tool_script={'run_sql_query': [
        ('run_sql_query', {'sql': 'SELECT supplier_id, risk_rating FROM gold_dim_supplier'}),
        ('run_sql_query', {'sql': APAC})]},
        composer=lambda ev, n: f"There are {ev[0]['result']['rows'][0]['supplier_count']} suppliers in APAC.")
    r = ask(client, llm, 'How many suppliers are in APAC?')
    assert [e['status'] for e in r['evidence']] == ['error', 'ok']
    assert 'is confidential' in r['evidence'][0]['result_summary']['error']
    assert r['status'] == 'answered' and r['quality']['passed']


def test_truncated_sql_result_in_the_fallback(client):
    llm = FakeLLM(specialists=['sql_analyst'], composer=lambda ev, n: 'There are 999 order lines.',
                  tool_script={'run_sql_query': [('run_sql_query', {'sql': PO_LINES, 'limit': 3})]})
    r = ask(client, llm, 'List the purchase-order lines')
    assert r['quality']['fallback_used'] and '999' not in r['answer']
    assert 'run_sql_query: 3 rows returned (cut at the row limit)' in r['answer']
    assert any('cut at the row limit' in x for x in r['limitations'])


# --------------------------------------------------------------------------- consistency
def test_only_sql_analyst_has_the_sql_tool():
    assert [s for s, tools in SPECIALIST_TOOLS.items() if 'run_sql_query' in tools] == ['sql_analyst']


def test_prompt_schema_matches_the_endpoint_allowlist():
    """The agent sees exactly the tables and columns the endpoint accepts, and no confidential column."""
    lines = {line.split(':', 1)[0][2:]: line for line in prompts.SQL_SCHEMA.splitlines() if line.startswith('- gold_')}
    assert set(lines) == set(module.SQL_TABLES)
    for table, columns in module.SQL_TABLES.items():
        assert set(columns) <= set(re.findall(r'[a-z_]+', lines[table])), table
    assert not module.SQL_CONFIDENTIAL_COLUMNS & set(re.findall(r'[a-z_]+', prompts.SQL_SCHEMA))
