"""Test setup: real Gold tables (tests/data/gold, exported from the pipeline), the API's SQL executed by local
Spark, Entra ID access tokens signed with a local test key, and a scripted fake LLM in place of Azure OpenAI."""
import json
import os
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import dotenv
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

TENANT, CLIENT = '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222'
os.environ.update(DATABRICKS_HOST='https://example', DATABRICKS_TOKEN='x', DATABRICKS_WAREHOUSE_ID='w',
                  ENTRA_TENANT_ID=TENANT, ENTRA_API_CLIENT_ID=CLIENT, ENTRA_REQUIRED_APP_ROLE='SupplyChain.Read')
dotenv.load_dotenv = lambda *args, **kwargs: False   # a developer's local .env must not override the values above
HERE = Path(__file__).resolve().parent
GOLD_DIR = HERE / 'data' / 'gold'          # one CSV per Gold table used by the API
sys.path.insert(0, str(HERE.parent))

from langchain_core.messages import AIMessage, ToolMessage

from assistant.schemas import Draft, Judgment, RouteDecision, SupervisorDecision

# --------------------------------------------------------------------------- Entra ID test tokens
SIGNING_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def token(**overrides):
    """A v2.0 delegated access token for the API, as Entra ID would issue it; overrides change or drop (None) claims."""
    now = int(time.time())
    claims = {'aud': CLIENT, 'iss': f'https://login.microsoftonline.com/{TENANT}/v2.0', 'tid': TENANT,
              'iat': now, 'nbf': now, 'exp': now + 3600, 'scp': 'access_as_user', 'name': 'Test User'} | overrides
    return jwt.encode({k: v for k, v in claims.items() if v is not None}, SIGNING_KEY, algorithm='RS256',
                      headers={'kid': 'test'})


def bearer(**overrides):
    return {'Authorization': f'Bearer {token(**overrides)}'}


H = bearer()


class _LocalKeys:
    """Stands in for the tenant's JWKS endpoint."""

    def get_signing_key_from_jwt(self, _):
        return jwt.PyJWK.from_dict(json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(SIGNING_KEY.public_key())))


import app  # noqa: E402

app.jwks_client = _LocalKeys()   # tokens are checked against the local test key, also without Spark


@pytest.fixture(scope='session')
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master('local[2]').config('spark.ui.enabled', 'false')
         .config('spark.ui.showConsoleProgress', 'false').config('spark.sql.ansi.enabled', 'true')
         .config('spark.sql.session.timeZone', 'UTC').config('spark.driver.memory', '3g').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    for csv in GOLD_DIR.glob('gold_*.csv'):
        s.read.csv(str(csv), header=True, inferSchema=True).createOrReplaceTempView(csv.stem)
    return s


@pytest.fixture(scope='session')
def api(spark):
    import app as module

    async def local_run_statement(client, statement, parameters=None):
        sql = statement.replace(f'{module.CATALOG}.{module.SCHEMA}.', '')
        args = {p['name']: date.fromisoformat(p['value']) if p['type'] == 'DATE' else p['value']
                for p in parameters or []}
        conv = lambda v: float(v) if isinstance(v, Decimal) else v.isoformat() if isinstance(v, date) else v
        return [{k: conv(v) for k, v in r.asDict().items()} for r in spark.sql(sql, args=args).collect()]

    module.run_statement = local_run_statement
    module.jwks_client = _LocalKeys()
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
