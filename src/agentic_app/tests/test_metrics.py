"""POST /assistant/query reports the SQL statements, LLM calls and timings of the answer."""
import app as module
from conftest import H, FakeLLM

ORIGINAL_RUN_STATEMENT = module.run_statement      # captured before the test fixtures replace it

OTD = [('query_kpi', {'kpi': 'on_time_delivery_rate'})]


def test_answer_carries_sql_and_llm_metrics(client, api, monkeypatch):
    spark_stub = api.run_statement

    async def execute_on_spark(client, statement, parameters, metric):
        metric['statement_id'], metric['state'] = 'local', 'SUCCEEDED'
        return await spark_stub(client, statement, parameters)

    # The real run_statement (which records the metrics) over the local Spark tables.
    monkeypatch.setattr(api, 'run_statement', ORIGINAL_RUN_STATEMENT)
    monkeypatch.setattr(api, '_execute_statement', execute_on_spark)
    api.app.state.assistant_llm = FakeLLM(tool_script={'query_kpi': OTD}, composer=lambda ev, n: 'No numbers.')
    r = client.post('/assistant/query', json={'question': 'What is our on-time delivery rate?'}, headers=H)
    assert r.status_code == 200, r.text
    m = r.json()['metrics']

    assert m['sql_count'] == len(m['sql_statements']) >= 1
    assert {s['endpoint'] for s in m['sql_statements']} == {'/analytics/query'}     # query_kpi's endpoint
    assert all(s['state'] == 'SUCCEEDED' and s['duration_ms'] >= 0 for s in m['sql_statements'])
    assert m['sql_total_ms'] <= m['total_ms'] * len(m['sql_statements'])
    assert m['sql_rows'] == sum(s['rows'] for s in m['sql_statements'])
    assert m['tool_calls'] == 1


def test_statement_outside_an_assistant_question_is_not_collected():
    assert module.sql_metrics.get() is None


def test_llm_calls_are_recorded_with_node_time_and_tokens():
    import asyncio

    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage

    from assistant.metrics import track_llm_calls

    reply = AIMessage('ok', usage_metadata={'input_tokens': 12, 'output_tokens': 3, 'total_tokens': 15})
    llm = GenericFakeChatModel(messages=iter([reply, reply, reply]))
    with track_llm_calls() as recorder:
        asyncio.run(llm.ainvoke('hi', config={'metadata': {'langgraph_node': 'judge'}}))
        llm.invoke('hi')
    llm.invoke('outside')                              # after the block: not recorded
    assert [c['node'] for c in recorder.calls] == ['judge', '-']
    assert all(c['status'] == 'ok' and c['duration_ms'] >= 0 for c in recorder.calls)
    assert [(c['input_tokens'], c['output_tokens']) for c in recorder.calls] == [(12, 3), (12, 3)]
