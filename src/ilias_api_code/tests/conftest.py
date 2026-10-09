"""Test setup: real Gold tables (built locally by the pipeline), the API's SQL executed by local Spark,
and a scripted fake LLM in place of Azure OpenAI."""
import json
import os
import sys
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.update(DATABRICKS_HOST='https://example', DATABRICKS_TOKEN='x', DATABRICKS_WAREHOUSE_ID='w',
                  API_KEY='secret')
HERE = Path(__file__).resolve().parent
# The Gold tables are built with the pipeline repository (team4_supply_chain) from the generated data files.
PIPELINE_REPO = Path(os.environ.get('PIPELINE_REPO', HERE.parent.parent / 'team4_supply_chain'))
DATA_DIR = os.environ.get('BRONZE_TEST_DATA', str(PIPELINE_REPO / 'data'))
sys.path[:0] = [str(HERE.parent), str(PIPELINE_REPO / 'src'), str(PIPELINE_REPO / 'tests')]

from langchain_core.messages import AIMessage, ToolMessage

from assistant.schemas import Draft, Judgment, RouteDecision, SupervisorDecision


@pytest.fixture(scope='session')
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master('local[2]').config('spark.ui.enabled', 'false')
         .config('spark.ui.showConsoleProgress', 'false').config('spark.sql.ansi.enabled', 'true')
         .config('spark.sql.session.timeZone', 'UTC').config('spark.driver.memory', '3g').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    from silver_harness import make_landing, run_on_landing
    tables = run_on_landing(s, make_landing(DATA_DIR, Path(tempfile.mkdtemp())))['tables']
    for name, df in tables.items():
        if name.startswith('gold_'):
            df.createOrReplaceTempView(name)
    return s


@pytest.fixture(scope='session')
def api(spark):
    import supply_chain_api as module

    async def local_run_statement(client, statement, parameters=None):
        sql = statement.replace(f'{module.CATALOG}.{module.SCHEMA}.', '')
        args = {p['name']: date.fromisoformat(p['value']) if p['type'] == 'DATE' else p['value']
                for p in parameters or []}
        conv = lambda v: float(v) if isinstance(v, Decimal) else v.isoformat() if isinstance(v, date) else v
        return [{k: conv(v) for k, v in r.asDict().items()} for r in spark.sql(sql, args=args).collect()]

    module.run_statement = local_run_statement
    return module


@pytest.fixture
def client(api):
    from fastapi.testclient import TestClient
    with TestClient(api.app) as c:
        yield c
    api.app.state.assistant_llm = None


# --------------------------------------------------------------------------- scripted fake LLM
class _Runner:
    def __init__(self, fn):
        self.fn = fn

    async def ainvoke(self, messages):
        return self.fn(messages)


def evidence_from(messages):
    text = messages[-1].content
    return json.loads(text.split('EVIDENCE_JSON:\n', 1)[1].split('\n\nSpecialist findings', 1)[0])


class FakeLLM:
    """Stand-in for AzureChatOpenAI with the same interface the graph uses.

    route:       RouteDecision route for the guard
    specialists: list of specialist names the supervisor delegates to, in order, then 'finish'
    tool_script: {tool_name_available_to_specialist: [(tool, args), ...]} calls made one per turn
    composer:    function(evidence, attempt) -> answer text
    judge:       function(attempt) -> dict of scores (default all 90)
    """

    def __init__(self, route='answer', specialists=('kpi_analyst',), tool_script=None, composer=None, judge=None):
        self.route, self.specialists = route, list(specialists)
        self.tool_script = tool_script or {}
        self.composer_fn = composer or (lambda ev, attempt: 'No numbers here.')
        self.judge_fn = judge or (lambda attempt: {})
        self.calls, self.composer_attempts, self.judge_attempts = [], 0, 0

    def with_structured_output(self, schema, **kwargs):
        handlers = {'RouteDecision': self._route, 'SupervisorDecision': self._supervise,
                    'Draft': self._compose, 'Judgment': self._judge}
        return _Runner(lambda msgs: (self.calls.append(schema.__name__), handlers[schema.__name__](msgs))[1])

    def bind_tools(self, tools, **kwargs):
        return _Runner(lambda msgs: self._specialist(msgs, [t['function']['name'] for t in tools]))

    async def ainvoke(self, messages):
        return self._specialist(messages, [])

    def _route(self, msgs):
        return RouteDecision(route=self.route, reason='scripted', clarifying_question='Which KPI do you mean?')

    def _supervise(self, msgs):
        done = msgs[-1].content.count('\n- ')
        nxt = self.specialists[done] if done < len(self.specialists) else 'finish'
        return SupervisorDecision(next=nxt, task=f'task for {nxt}' if nxt != 'finish' else '')

    def _specialist(self, msgs, tool_names):
        used = sum(isinstance(m, ToolMessage) for m in msgs)
        script = next((self.tool_script[n] for n in tool_names if n in self.tool_script), [])
        if tool_names and used < len(script):
            name, args = script[used]
            return AIMessage(content='', tool_calls=[{'name': name, 'args': args, 'id': f'call_{used}'}])
        return AIMessage(content='Findings summarised from the tool results.')

    def _compose(self, msgs):
        self.composer_attempts += 1
        answer = self.composer_fn(evidence_from(msgs), self.composer_attempts)
        return Draft(status='answered', answer=answer, measures=['scripted'], limitations=[])

    def _judge(self, msgs):
        self.judge_attempts += 1
        scores = {k: 90 for k in ('correctness', 'groundedness', 'faithfulness', 'relevance', 'completeness',
                                  'clarity')} | self.judge_fn(self.judge_attempts)
        if scores.pop('raise', False):
            raise RuntimeError('judge unavailable')
        return Judgment(**scores, issues=['scripted issue'] if min(scores.values()) < 80 else [], reasoning='scripted')
