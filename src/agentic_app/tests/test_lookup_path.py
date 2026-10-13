"""SQL lookup path: guard (lookup) -> entity_resolver -> schema_retriever -> sql_writer -> sql_validator ->
execute_sql -> result_check -> composer, plus GET /analytics/sql/schema and POST /analytics/sql/validate.

Expected values come from the Gold CSV fixtures (tests/data/gold): PRD-00001 is 'Label Roll C35'."""
import json

from conftest import H, FakeLLM

import app as module
from assistant import prompts
from assistant.lookup import resolve_entities

NAME_SQL = "SELECT product_id, product_name FROM gold_dim_product WHERE product_id = 'PRD-00001'"
RANDOM_SQL = ('SELECT product_id, product_name, alternate_supplier_id FROM gold_dim_product '
              'WHERE alternate_supplier_id IS NOT NULL AND product_key <> -1 ORDER BY rand() LIMIT 1')
LOOKUP_PATH = ['guard:answer:lookup', 'entity_resolver:PRD-00001', 'schema_retriever', 'sql_writer:single',
               'sql_validator:ok', 'execute_sql:ok', 'result_check:ok', 'composer', 'checks:pass', 'judge:pass',
               'finalize']


def name_answer(ev, attempt):
    row = ev[-1]['result']['rows'][0]
    return f"{row['product_id']} is called {row['product_name']}."


def ask(client, llm, question):
    module.app.state.assistant_llm = llm
    response = client.post('/assistant/query', json={'question': question}, headers=H)
    assert response.status_code == 200, response.text
    return response.json()


# --------------------------------------------------------------------------- API: schema and dry run
def test_schema_lists_the_allowlist_without_confidential_columns(client):
    schema = client.get('/analytics/sql/schema', headers=H).json()
    assert {t['name']: t['columns'] for t in schema['tables']} == module.SQL_TABLES
    assert 'Packaging' in schema['allowed_values']['gold_dim_product.category']      # read from Gold
    assert 'alternate_supplier_id IS NOT NULL' in schema['synonyms']['has an alternate (backup) supplier']
    text = json.dumps(schema)
    assert not [c for c in module.SQL_CONFIDENTIAL_COLUMNS if c in text]
    assert client.get('/analytics/sql/schema').status_code == 401


def test_dry_run_accepts_a_valid_query(client):
    body = client.post('/analytics/sql/validate', json={'sql': NAME_SQL}, headers=H).json()
    assert body['valid'] and body['columns'] == ['product_id', 'product_name'] and 'rows' not in body
    assert f'{module.CATALOG}.{module.SCHEMA}.gold_dim_product' in body['sql']


def test_dry_run_reports_unknown_columns_with_the_valid_ones(client):
    response = client.post('/analytics/sql/validate', headers=H,
                           json={'sql': 'SELECT product_title FROM gold_dim_product'})
    assert response.status_code == 422
    assert "Unknown column 'product_title'" in response.json()['detail'] and 'product_name' in response.json()['detail']
    run = client.post('/analytics/sql', headers=H, json={'sql': 'SELECT product_title FROM gold_dim_product'})
    assert run.status_code == 422                                  # same rule on execution


def test_dry_run_catches_errors_only_the_warehouse_sees(client):
    response = client.post('/analytics/sql/validate', headers=H,
                           json={'sql': 'SELECT category, product_id FROM gold_dim_product GROUP BY category'})
    assert response.status_code == 422
    assert 'does not run' in response.json()['detail'] and 'MISSING_AGGREGATION' in response.json()['detail']


def test_dry_run_keeps_the_safety_rules(client):
    for sql in ('DELETE FROM gold_dim_product', 'SELECT risk_rating FROM gold_dim_supplier', 'SELECT * FROM gold_dim_product'):
        assert client.post('/analytics/sql/validate', json={'sql': sql}, headers=H).status_code == 422


def test_warehouse_errors_are_redacted():
    error = ('org.apache.spark.sql.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column `rsk` cannot be '
             'resolved. Did you mean one of the following? [`risk_rating`, `unit_cost`]')
    assert module.redacted(error).startswith('[UNRESOLVED_COLUMN.WITH_SUGGESTION]')
    assert 'risk_rating' not in module.redacted(error) and 'unit_cost' not in module.redacted(error)


# --------------------------------------------------------------------------- entity resolver
def test_entity_resolver_normalises_ids_by_their_first_segment():
    found = resolve_entities('name of prd-00001 and SITE-SUP-0040, PO-2025-00208, top-5')
    assert [(e['id'], e['table'], e['column']) for e in found] == [
        ('PRD-00001', 'gold_dim_product', 'product_id'),
        ('SITE-SUP-0040', 'gold_dim_location', 'location_id'),
        ('PO-2025-00208', 'gold_fact_purchase_order_line', 'po_id')]


# --------------------------------------------------------------------------- assistant: lookup path
def test_name_of_a_product(client):
    llm = FakeLLM(question_type='lookup', sql_writer=[NAME_SQL], composer=name_answer)
    r = ask(client, llm, 'Which is the name of PRD-00001?')
    assert r['status'] == 'answered' and r['answer'] == 'PRD-00001 is called Label Roll C35.'
    assert r['trace'] == LOOKUP_PATH and 'SupervisorDecision' not in llm.calls      # supervisor skipped
    assert 'PRD-00001: gold_dim_product.product_id' in llm.writer_inputs[0]          # resolved in code
    assert 'Packaging' in llm.writer_inputs[0]                                       # schema from the API
    assert llm.prompts['Draft'] == [prompts.COMPOSER_LOOKUP] and llm.prompts['Judgment'] == [prompts.JUDGE_LOOKUP]
    assert r['quality']['passed'] and r['evidence'][0]['tool'] == 'run_sql_query'


def test_random_product_with_an_alternate_supplier(client):
    def answer(ev, attempt):
        row = ev[-1]['result']['rows'][0]
        return (f"A random example: {row['product_id']} ({row['product_name']}), "
                f"alternate supplier {row['alternate_supplier_id']}.")
    llm = FakeLLM(question_type='lookup', sql_writer=[RANDOM_SQL], composer=answer)
    r = ask(client, llm, 'Give me a random product with its alternate supplier.')
    row = r['evidence'][0]['result_summary']['rows'][0]
    assert r['status'] == 'answered' and r['quality']['passed'] and 'sql_writer:sample' in r['trace']
    assert row['alternate_supplier_id'] and row['product_id'] != 'UNKNOWN'
    assert row['product_id'] in r['answer'] and row['alternate_supplier_id'] in r['answer']


def test_validator_error_goes_back_to_the_writer(client):
    llm = FakeLLM(question_type='lookup', composer=name_answer,
                  sql_writer=["SELECT product_id, product_title FROM gold_dim_product WHERE product_id = 'PRD-00001'",
                              NAME_SQL])
    r = ask(client, llm, 'Which is the name of PRD-00001?')
    assert 'PREVIOUS ATTEMPT' in llm.writer_inputs[1] and "Unknown column 'product_title'" in llm.writer_inputs[1]
    assert [(e['tool'], e['status']) for e in r['evidence']] == [('validate_sql_query', 'error'),
                                                                 ('run_sql_query', 'ok')]
    assert r['status'] == 'answered' and r['answer'] == 'PRD-00001 is called Label Roll C35.'


def test_writer_gives_up_after_three_invalid_queries(client):
    llm = FakeLLM(question_type='lookup', sql_writer=['SELECT product_title FROM gold_dim_product'])
    r = ask(client, llm, 'Which is the name of PRD-00001?')
    assert r['trace'].count('sql_validator:fail') == 3 and not any(t.startswith('execute_sql') for t in r['trace'])
    assert r['status'] == 'no_data'


def test_empty_result_gets_one_retry(client):
    def answer(ev, attempt):
        assert len(ev) == 1                                    # only the final SQL result is evidence
        return name_answer(ev, attempt)
    llm = FakeLLM(question_type='lookup', composer=answer,
                  sql_writer=[NAME_SQL.replace("'PRD-00001'", "'prd-00001'"), NAME_SQL])
    r = ask(client, llm, 'Which is the name of PRD-00001?')
    assert 'result_check:empty' in r['trace'] and r['trace'].count('execute_sql:ok') == 2
    assert '0 rows' in llm.writer_inputs[1] and r['answer'] == 'PRD-00001 is called Label Roll C35.'


def test_nothing_matches_is_an_answer(client):
    missing = NAME_SQL.replace('PRD-00001', 'PRD-99999')
    llm = FakeLLM(question_type='lookup', sql_writer=[missing],
                  composer=lambda ev, n: 'No product matches PRD-99999.')
    r = ask(client, llm, 'Which is the name of PRD-99999?')
    assert r['trace'].count('result_check:empty') == 2 and r['status'] == 'answered'
    assert r['answer'] == 'No product matches PRD-99999.' and r['quality']['passed']


def test_ids_not_in_the_rows_are_caught(client):
    llm = FakeLLM(question_type='lookup', sql_writer=[NAME_SQL],
                  composer=lambda ev, n: ('PRD-00001 is called Label Roll C35, like PRD-00002.' if n == 1
                                          else name_answer(ev, n)))
    r = ask(client, llm, 'Which is the name of PRD-00001?')
    assert 'checks:fail' in r['trace'] and r['quality']['revised'] and r['quality']['passed']
    assert 'PRD-00002' not in r['answer']


def test_lookup_when_the_data_service_is_down(client, api, monkeypatch):
    from fastapi import HTTPException

    async def down(*args, **kwargs):
        raise HTTPException(status_code=502, detail='warehouse down')
    monkeypatch.setattr(api, 'run_statement', down)
    r = ask(client, FakeLLM(question_type='lookup', sql_writer=[NAME_SQL]), 'Which is the name of PRD-00001?')
    assert r['status'] == 'data_unavailable' and r['trace'][-2:] == ['schema_retriever:error', 'unavailable']
