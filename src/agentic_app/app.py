"""Team 4 Supply Chain Disruption API: business questions answered from the Databricks Gold layer.

Run:
    uv sync                      # also makes the knowledge_graph package (src/knowledge_graph) importable
    uv run python app.py

The knowledge graph is read from KG_PATH (default: <repo>/data/graph/supply_graph.json); rebuild it with
`uv run python -m knowledge_graph.build_knowledge_graph` and the API picks up the new file without a restart.

Credentials are read from a .env file next to this file (see .env.example).
Callers sign in with Microsoft Entra ID and send the access token as 'Authorization: Bearer <token>'.
User interface: http://127.0.0.1:8000/   Interactive docs: http://127.0.0.1:8000/docs
"""
import asyncio
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import date
from pathlib import Path
from typing import Any, Literal

import httpx
import jwt
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, OAuth2AuthorizationCodeBearer
from pydantic import BaseModel, ConfigDict, Field

from assistant.graph import default_llm, run_assistant
from assistant.metrics import track_llm_calls
from assistant.schemas import AssistantAnswer, AssistantQuery, QueryMetrics
from gold_sql import UnsafeSql, prepare
from knowledge_graph.graph import load_graph, supplier_alternatives

load_dotenv(override=True)

HOST = os.environ['DATABRICKS_HOST'].strip().rstrip('/')
TOKEN = os.environ['DATABRICKS_TOKEN'].strip()
WAREHOUSE_ID = os.environ['DATABRICKS_WAREHOUSE_ID'].strip()
# Microsoft Entra ID: the app registration that exposes this API (see .env.example for the setup).
TENANT_ID = os.environ['ENTRA_TENANT_ID'].strip()
API_CLIENT_ID = os.environ['ENTRA_API_CLIENT_ID'].strip()
API_AUDIENCE = os.environ.get('ENTRA_API_AUDIENCE', f'api://{API_CLIENT_ID}').strip()
REQUIRED_SCOPE = os.environ.get('ENTRA_REQUIRED_SCOPE', 'access_as_user').strip()
REQUIRED_APP_ROLE = os.environ.get('ENTRA_REQUIRED_APP_ROLE', '').strip()   # optional: app-only (daemon) callers
UI_CLIENT_ID = os.environ.get('ENTRA_UI_CLIENT_ID', API_CLIENT_ID).strip()
CATALOG = os.environ.get('DATABRICKS_CATALOG', 'data2026acc123456').strip()
# Knowledge graph built from Gold by `python -m knowledge_graph.build_knowledge_graph` (repo data/graph by default).
KG_PATH = Path(os.environ.get('KG_PATH', '').strip()
               or Path(__file__).resolve().parents[2] / 'data' / 'graph' / 'supply_graph.json')
SCHEMA = os.environ.get('DATABRICKS_SCHEMA', 'team4').strip()

# Catalog and schema are inlined into SQL, so they must be plain identifiers.
for _identifier in (CATALOG, SCHEMA):
    if not re.fullmatch(r'[A-Za-z0-9_]+', _identifier):
        raise ValueError(f'Invalid catalog/schema name: {_identifier!r}')


def table(name: str) -> str:
    return f'{CATALOG}.{SCHEMA}.{name}'


FACT_PO_LINE = table('gold_fact_purchase_order_line')
FACT_EXPOSURE = table('gold_fact_inventory_exposure')
DIM_SUPPLIER = table('gold_dim_supplier')
DIM_PRODUCT = table('gold_dim_product')
DIM_LOCATION = table('gold_dim_location')
DIM_DATE = table('gold_dim_date')

# =============================================================================
# Structured logs with a correlation id (assignment 6.7)
# =============================================================================
CORRELATION_HEADER = 'X-Correlation-ID'
VALID_CORRELATION_ID = re.compile(r'[A-Za-z0-9._-]{1,64}')
correlation_id: ContextVar[str] = ContextVar('correlation_id', default='-')
# Per-question metrics: ask_assistant sets a list and run_statement appends one entry per SQL statement, tagged with
# the endpoint being served (the assistant's tool calls are in-process requests, so they share the list).
current_endpoint: ContextVar[str] = ContextVar('current_endpoint', default='-')
sql_metrics: ContextVar[list[dict[str, Any]] | None] = ContextVar('sql_metrics', default=None)


class CorrelationIdFilter(logging.Filter):
    """Stamps every log record with the correlation id of the request being served."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id.get()
        return True


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line. Only metadata is logged: never headers, request bodies or secrets."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            'ts': time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)) + f'.{int(record.msecs):03d}Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'correlation_id': getattr(record, 'correlation_id', '-'),
            **getattr(record, 'fields', {}),
        }
        if record.exc_info:
            entry['exception'] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


logger = logging.getLogger('supply_chain_api')
logger.setLevel(logging.INFO)
logger.addFilter(CorrelationIdFilter())
if not logger.handlers:
    _log_handler = logging.StreamHandler()
    _log_handler.setFormatter(JsonLogFormatter())
    logger.addHandler(_log_handler)

STATEMENTS_PATH = '/api/2.0/sql/statements'
POLL_DEADLINE_SECONDS = 120

# Bounded retries for transient Databricks errors (assignment 6.7): rate limit (429) and temporary unavailability.
RETRY_STATUSES = {429, 502, 503, 504}
MAX_ATTEMPTS = 3                     # 1 call + at most 2 retries
RETRY_BASE_DELAY_SECONDS = 1.0       # waits 1s, then 2s
MAX_RETRY_AFTER_SECONDS = 10.0       # upper bound for a Retry-After header sent by Databricks
retry_sleep = asyncio.sleep          # kept as a module attribute so tests can record the waits

# The Statement Execution API returns every value as a string (or null); convert by column type.
INT_TYPES = {'BYTE', 'SHORT', 'INT', 'LONG'}
FLOAT_TYPES = {'FLOAT', 'DOUBLE', 'DECIMAL'}

ID_PATTERN = r'^[A-Za-z0-9_\-]{1,40}$'
Region = Literal['EMEA', 'APAC', 'AMER']
Criticality = Literal['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']


# =============================================================================
# Databricks access
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient(
        base_url=HOST, headers={'Authorization': f'Bearer {TOKEN}'}, timeout=30,
    ) as client:
        app.state.client = client
        yield

def retry_delay(response: httpx.Response | None, attempt: int) -> float:
    """Seconds to wait before the next attempt: Retry-After when Databricks sends it, otherwise 1s, 2s, ..."""
    if response is not None:
        retry_after = response.headers.get('Retry-After', '')
        if retry_after.isdigit():
            return min(float(retry_after), MAX_RETRY_AFTER_SECONDS)
    return RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1)


async def send_with_retry(client: httpx.AsyncClient, method: str, url: str, **kwargs: Any) -> httpx.Response:
    """Sends one request to Databricks; retries 429/502/503/504 and network errors at most MAX_ATTEMPTS times."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.TransportError as exc:          # connection refused, timeout, connection reset ...
            if attempt == MAX_ATTEMPTS:
                raise
            response, reason = None, type(exc).__name__
        else:
            if response.status_code not in RETRY_STATUSES or attempt == MAX_ATTEMPTS:
                return response
            reason = f'HTTP {response.status_code}'
        delay = retry_delay(response, attempt)
        logger.warning('Databricks %s; retry %s/%s in %.1fs', reason, attempt, MAX_ATTEMPTS - 1, delay)
        await retry_sleep(delay)
    raise AssertionError('unreachable')


class StatementFailed(HTTPException):
    """The SQL warehouse ran the statement and it failed (bad SQL), as opposed to the warehouse being unavailable."""

    def __init__(self, error_message: str):
        super().__init__(status_code=502, detail='The query on the data source failed. Please try again later.')
        self.error_message = error_message


def check_response(response: httpx.Response) -> dict[str, Any]:
    if response.is_error:
        # The full Databricks reply can contain SQL and table names: server log only, never to the client.
        logger.error('Databricks HTTP %s: %s', response.status_code, response.text[:2000])
        raise HTTPException(status_code=502, detail='The data source returned an error. Please try again later.')
    return response.json()


def convert(value: str | None, type_name: str) -> Any:
    if value is None:
        return None
    if type_name in INT_TYPES:
        return int(value)
    if type_name in FLOAT_TYPES:
        return float(value)
    if type_name == 'BOOLEAN':
        return value.lower() == 'true'
    return value


async def run_statement(client: httpx.AsyncClient, statement: str,
                        parameters: list[dict[str, str]] | None = None) -> list[dict[str, Any]]:
    """Runs one SQL statement on the SQL warehouse; user values are passed only as named parameters."""
    metric = {'endpoint': current_endpoint.get(), 'statement_id': None, 'state': 'ERROR', 'rows': 0, 'polls': 0}
    start = time.perf_counter()
    try:
        rows = await _execute_statement(client, statement, parameters, metric)
        metric['rows'] = len(rows)
        return rows
    finally:
        metric['duration_ms'] = round((time.perf_counter() - start) * 1000, 1)
        logger.info('sql statement', extra={'fields': metric})
        if (collected := sql_metrics.get()) is not None:
            collected.append(metric)


async def _execute_statement(client: httpx.AsyncClient, statement: str, parameters: list[dict[str, str]] | None,
                             metric: dict[str, Any]) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {
        'warehouse_id': WAREHOUSE_ID, 'statement': statement, 'wait_timeout': '10s',
        'disposition': 'INLINE', 'format': 'JSON_ARRAY',
    }
    if parameters:
        payload['parameters'] = parameters
    result = check_response(await send_with_retry(client, 'POST', STATEMENTS_PATH, json=payload))
    statement_id = metric['statement_id'] = result['statement_id']

    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    while result['status']['state'] in {'PENDING', 'RUNNING'}:
        if time.monotonic() >= deadline:
            try:
                await client.post(f'{STATEMENTS_PATH}/{statement_id}/cancel')
            except httpx.HTTPError:
                pass
            metric['state'] = 'TIMEOUT'
            raise HTTPException(status_code=504, detail=f'Statement {statement_id} timed out and was cancelled.')
        await asyncio.sleep(1)
        metric['polls'] += 1
        result = check_response(await send_with_retry(client, 'GET', f'{STATEMENTS_PATH}/{statement_id}'))

    metric['state'] = result['status']['state']
    if result['status']['state'] != 'SUCCEEDED':
        error = result['status'].get('error') or {}
        logger.error('Statement %s ended in state %s: %s', statement_id, result['status']['state'], error)
        raise StatementFailed(str(error.get('message', result['status']['state'])))

    schema = result['manifest']['schema']['columns']
    chunk = result.get('result', {})
    rows = list(chunk.get('data_array', []))
    while link := chunk.get('next_chunk_internal_link'):
        chunk = check_response(await send_with_retry(client, 'GET', link))
        rows.extend(chunk.get('data_array', []))
    return [{col['name']: convert(v, col['type_name']) for col, v in zip(schema, row)} for row in rows]


class Filters:
    """Builds a WHERE clause; every user value becomes a named parameter (never inlined)."""

    def __init__(self, *fixed_conditions: str):
        self.conditions = list(fixed_conditions)
        self.parameters: list[dict[str, str]] = []
        self.applied: dict[str, Any] = {}

    def add(self, name: str, condition: str, value: Any, sql_type: str = 'STRING') -> None:
        if value is None:
            return
        self.conditions.append(condition)
        self.parameters.append({'name': name, 'value': str(value), 'type': sql_type})
        self.applied[name] = value if not isinstance(value, date) else value.isoformat()

    def sql(self) -> str:
        return ('WHERE ' + ' AND '.join(self.conditions)) if self.conditions else ''


# =============================================================================
# App, security, errors
# =============================================================================
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
SCOPE = f'{API_AUDIENCE}/{REQUIRED_SCOPE}'
# v2.0 tokens carry the client id as audience and the v2.0 issuer; v1.0 tokens the App ID URI and sts.windows.net.
AUDIENCES = [API_AUDIENCE, API_CLIENT_ID]
ISSUERS = [f'{AUTHORITY}/v2.0', f'https://sts.windows.net/{TENANT_ID}/']

app = FastAPI(
    title='Team 4 Supply Chain Disruption API',
    version='1.0.0',
    description='Answers the business questions of the Team 4 business brief from the Gold layer in Databricks. '
                'Monetary values are in EUR. Every response carries as_of_date, the snapshot date of the data. '
                'Sign in with Microsoft Entra ID (Authorize button) to call the endpoints.',
    lifespan=lifespan,
    swagger_ui_init_oauth={'clientId': UI_CLIENT_ID, 'usePkceWithAuthorizationCodeGrant': True, 'scopes': SCOPE},
)


@app.middleware('http')
async def correlation_id_and_access_log(request: Request, call_next):
    """Gives every request a correlation id (taken from the X-Correlation-ID header when valid, otherwise new),
    returns it in the response header and writes one structured log line per request."""
    incoming = request.headers.get(CORRELATION_HEADER, '')
    request_id = incoming if VALID_CORRELATION_ID.fullmatch(incoming) else uuid.uuid4().hex
    token = correlation_id.set(request_id)
    endpoint_token = current_endpoint.set(request.url.path)
    start = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        response.headers[CORRELATION_HEADER] = request_id
        return response
    finally:
        logger.info('request', extra={'fields': {
            'method': request.method, 'path': request.url.path, 'status': status,
            'duration_ms': round((time.perf_counter() - start) * 1000, 1)}})
        current_endpoint.reset(endpoint_token)
        correlation_id.reset(token)


# Signing keys of the tenant, fetched once and cached (refetched when Entra rotates them).
jwks_client = jwt.PyJWKClient(f'{AUTHORITY}/discovery/v2.0/keys', cache_keys=True, lifespan=3600)

# Documents the sign-in flow in OpenAPI (Swagger "Authorize"); HTTPBearer reads the token itself.
entra_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl=f'{AUTHORITY}/oauth2/v2.0/authorize', tokenUrl=f'{AUTHORITY}/oauth2/v2.0/token',
    scopes={SCOPE: 'Read supply chain data'}, auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)


def require_user(credentials: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
                 _: str | None = Security(entra_scheme)) -> dict[str, Any]:
    """Validates an Entra ID access token for this API: signature, audience, issuer, expiry and permission.
    A plain def, so FastAPI runs it in a worker thread and a key download never blocks the event loop."""
    unauthorized = HTTPException(status_code=401, detail='Missing or invalid Microsoft Entra ID access token.',
                                 headers={'WWW-Authenticate': 'Bearer'})
    if credentials is None:
        raise unauthorized
    try:
        key = jwks_client.get_signing_key_from_jwt(credentials.credentials).key
        claims = jwt.decode(credentials.credentials, key, algorithms=['RS256'], audience=AUDIENCES, issuer=ISSUERS,
                            options={'require': ['exp', 'iat', 'aud', 'iss', 'tid']})
    except (jwt.PyJWTError, jwt.PyJWKClientError):
        raise unauthorized
    if claims['tid'] != TENANT_ID:
        raise unauthorized
    delegated = REQUIRED_SCOPE in claims.get('scp', '').split()
    app_only = bool(REQUIRED_APP_ROLE) and REQUIRED_APP_ROLE in claims.get('roles', [])
    if not (delegated or app_only):
        needed = f"the '{REQUIRED_SCOPE}' scope" + (f" or the '{REQUIRED_APP_ROLE}' app role" if REQUIRED_APP_ROLE else '')
        raise HTTPException(status_code=403, detail=f'The token lacks {needed}.')
    return claims


@app.exception_handler(httpx.HTTPError)
async def httpx_error_handler(request: Request, exc: httpx.HTTPError):
    return JSONResponse(status_code=502, content={'detail': f'Could not reach Databricks: {type(exc).__name__}'})


# =============================================================================
# KPIs (business_brief.md section 4)
# =============================================================================
KpiName = Literal['order_value', 'on_time_delivery_rate', 'average_delay_days', 'supplier_fill_rate',
                  'exposed_value', 'delayed_quantity', 'critical_item_count', 'alternate_supplier_coverage']

# name -> (fact, unit, brief definition, SQL aggregate). 'po' = PO lines, 'exposure' = products at snapshot.
KPIS: dict[str, tuple[str, str, str, str]] = {
    'order_value': (
        'po', 'EUR', 'Total value of purchase-order lines: SUM(ordered_qty x unit_price x rate_to_eur)',
        'ROUND(SUM(f.ordered_qty * f.unit_price * f.rate_to_eur), 2)'),
    'on_time_delivery_rate': (
        'po', 'ratio', 'Share of delivered lines received on or before the promised date',
        'ROUND(COUNT_IF(f.is_on_time) / NULLIF(COUNT_IF(f.is_delivered), 0), 4)'),
    'average_delay_days': (
        'po', 'days', 'Average lateness of delivered lines that arrived late',
        'ROUND(AVG(CASE WHEN f.is_delivered AND f.delay_days > 0 THEN f.delay_days END), 2)'),
    'supplier_fill_rate': (
        'po', 'ratio', 'Share of ordered quantity received for lines past their promised date',
        ('ROUND(SUM(CASE WHEN f.promised_delivery_date_key <= snap.k THEN LEAST(f.received_qty, f.ordered_qty) END)'
         ' / NULLIF(SUM(CASE WHEN f.promised_delivery_date_key <= snap.k THEN f.ordered_qty END), 0), 4)')),
    'delayed_quantity': (
        'po', 'units', 'Units not received by the promised date, plus open units with an ETA after the promised date',
        'ROUND(SUM(f.delayed_qty), 2)'),
    'exposed_value': (
        'exposure', 'EUR',
        'Value of delayed inbound goods for products that will run out of usable stock before arrival',
        ('ROUND(SUM(CASE WHEN e.days_of_cover < e.days_until_arrival '
         'THEN e.delayed_inbound_qty * e.unit_cost * e.rate_to_eur ELSE 0 END), 2)')),
    'critical_item_count': (
        'exposure', 'products', 'Number of CRITICAL products with at least one delayed open inbound line',
        "COUNT(DISTINCT CASE WHEN p.criticality = 'CRITICAL' AND e.delayed_inbound_qty > 0 THEN p.product_id END)"),
    'alternate_supplier_coverage': (
        'exposure', 'ratio', 'Share of CRITICAL products with an active alternate supplier different from the primary',
        ("ROUND(COUNT_IF(p.criticality = 'CRITICAL' AND p.has_active_alternate) "
         "/ NULLIF(COUNT_IF(p.criticality = 'CRITICAL'), 0), 4)")),
}

SNAPSHOT_CTE = (f'WITH snap AS (SELECT MAX(x.snapshot_date_key) AS k, MAX(d.date) AS as_of_date '
                f'FROM {FACT_EXPOSURE} x JOIN {DIM_DATE} d ON x.snapshot_date_key = d.date_key)')

FROM_PO_LINES = f'''
    FROM {FACT_PO_LINE} f
    JOIN {DIM_SUPPLIER} s ON f.supplier_key = s.supplier_key
    JOIN {DIM_PRODUCT} p ON f.product_key = p.product_key
    JOIN {DIM_LOCATION} l ON f.location_key = l.location_key
    JOIN {DIM_DATE} od ON f.order_date_key = od.date_key
    CROSS JOIN snap'''

FROM_EXPOSURE = f'''
    FROM {FACT_EXPOSURE} e
    JOIN {DIM_PRODUCT} p ON e.product_key = p.product_key
    JOIN {DIM_SUPPLIER} s ON e.supplier_key = s.supplier_key
    JOIN {DIM_LOCATION} l ON e.location_key = l.location_key
    CROSS JOIN snap'''


class KpiQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')

    kpi: KpiName = Field(description='Business brief KPI to compute.')
    group_by: Literal['none', 'month', 'region'] = Field(
        'none', description="'month' = order month (trend; purchase-order KPIs only), 'region' = supplier region.")
    supplier_id: str | None = Field(None, pattern=ID_PATTERN, description='Only this supplier, e.g. SUP-0007.')
    product_id: str | None = Field(None, pattern=ID_PATTERN, description='Only this product, e.g. PRD-00080.')
    region: Region | None = Field(None, description='Only suppliers in this region.')
    category: str | None = Field(None, max_length=60, description='Only products of this category.')
    criticality: Criticality | None = Field(None, description='Only products with this criticality.')
    location_id: str | None = Field(None, pattern=ID_PATTERN,
                                    description='Only this warehouse (delivery location for purchase-order KPIs, '
                                                'stocking location for exposure KPIs), e.g. WH-MIL-01.')
    order_date_from: date | None = Field(None, description='Purchase-order KPIs only: order_date on or after.')
    order_date_to: date | None = Field(None, description='Purchase-order KPIs only: order_date on or before.')


class KpiValue(BaseModel):
    group: str = Field(description="Group label: 'all', an order month (yyyy-MM) or a region.")
    value: float | None = Field(description='KPI value; null when it cannot be computed (no rows).')


class KpiResponse(BaseModel):
    kpi: str
    definition: str
    unit: str
    as_of_date: str | None = Field(description='Snapshot date of the data (yyyy-MM-dd).')
    filters: dict[str, Any]
    results: list[KpiValue]


@app.post('/analytics/query', response_model=KpiResponse, operation_id='query_kpi',
          summary='Compute one business KPI, optionally filtered and grouped',
          dependencies=[Depends(require_user)])
async def query_kpi(request: Request, body: KpiQuery) -> KpiResponse:
    """Computes one of the 8 business brief KPIs (Order Value, On-Time Delivery Rate, Average Delay,
    Supplier Fill Rate, Exposed Value, Delayed Quantity, Critical-Item Count, Alternate-Supplier Coverage).
    Use group_by='month' to see whether performance is improving over time."""
    fact, unit, definition, aggregate = KPIS[body.kpi]
    is_po = fact == 'po'
    if not is_po and (body.group_by == 'month' or body.order_date_from or body.order_date_to):
        raise HTTPException(status_code=422, detail=f"{body.kpi} is a snapshot KPI: order dates and "
                                                    "group_by='month' do not apply.")
    where = Filters()
    where.add('supplier_id', 's.supplier_id = :supplier_id', body.supplier_id)
    where.add('product_id', 'p.product_id = :product_id', body.product_id)
    where.add('region', 's.region = :region', body.region)
    where.add('category', 'p.category = :category', body.category)
    where.add('criticality', 'p.criticality = :criticality', body.criticality)
    where.add('location_id', 'l.location_id = :location_id', body.location_id)
    where.add('order_date_from', 'od.date >= :order_date_from', body.order_date_from, 'DATE')
    where.add('order_date_to', 'od.date <= :order_date_to', body.order_date_to, 'DATE')

    group = {'none': None, 'month': "date_format(od.date, 'yyyy-MM')", 'region': 's.region'}[body.group_by]
    statement = f'''
        {SNAPSHOT_CTE}
        SELECT {f"COALESCE(CAST({group} AS STRING), 'UNKNOWN')" if group else "'all'"} AS grp,
               {aggregate} AS value, MAX(snap.as_of_date) AS as_of_date
        {FROM_PO_LINES if is_po else FROM_EXPOSURE}
        {where.sql()}
        {f'GROUP BY {group} ORDER BY grp' if group else ''}'''
    rows = await run_statement(request.app.state.client, statement, where.parameters)
    return KpiResponse(
        kpi=body.kpi, definition=definition, unit=unit,
        as_of_date=rows[0]['as_of_date'] if rows else None, filters=where.applied,
        results=[KpiValue(group=r['grp'], value=r['value']) for r in rows],
    )


# =============================================================================
# Supplier scorecard
# =============================================================================
SCORECARD_SORT = {
    'on_time_delivery_rate': 'on_time_delivery_rate ASC',
    'supplier_fill_rate': 'supplier_fill_rate ASC',
    'average_delay_days': 'average_delay_days DESC',
    'delayed_quantity': 'delayed_quantity DESC',
    'order_value': 'order_value_eur DESC',
}


class SupplierScore(BaseModel):
    supplier_id: str
    supplier_name: str | None
    country_code: str | None
    region: str | None
    is_active: bool | None
    order_lines: int
    order_value_eur: float | None
    on_time_delivery_rate: float | None
    average_delay_days: float | None
    supplier_fill_rate: float | None
    delayed_quantity: float | None


class ScorecardResponse(BaseModel):
    as_of_date: str | None
    filters: dict[str, Any]
    count: int
    suppliers: list[SupplierScore]


@app.get('/suppliers/scorecard', response_model=ScorecardResponse, operation_id='get_supplier_scorecard',
         summary='Rank suppliers by delivery performance', dependencies=[Depends(require_user)])
async def get_supplier_scorecard(
    request: Request,
    sort_by: Literal['on_time_delivery_rate', 'supplier_fill_rate', 'average_delay_days', 'delayed_quantity',
                     'order_value'] = Query('on_time_delivery_rate',
                                            description='Ranking; the worst suppliers come first '
                                                        '(order_value: largest first).'),
    region: Region | None = Query(None, description='Only suppliers in this region.'),
    supplier_id: str | None = Query(None, pattern=ID_PATTERN, description='Only this supplier.'),
    order_date_from: date | None = Query(None, description='Only lines ordered on or after this date.'),
    order_date_to: date | None = Query(None, description='Only lines ordered on or before this date.'),
    limit: int = Query(10, ge=1, le=100, description='Maximum number of suppliers.'),
) -> ScorecardResponse:
    """Supplier scorecard for reviews and negotiations: On-Time Delivery Rate, Supplier Fill Rate,
    Average Delay, Delayed Quantity and Order Value per supplier. Answers which suppliers to escalate,
    review or replace."""
    where = Filters()
    where.add('region', 's.region = :region', region)
    where.add('supplier_id', 's.supplier_id = :supplier_id', supplier_id)
    where.add('order_date_from', 'od.date >= :order_date_from', order_date_from, 'DATE')
    where.add('order_date_to', 'od.date <= :order_date_to', order_date_to, 'DATE')
    statement = f'''
        {SNAPSHOT_CTE}
        SELECT s.supplier_id, s.supplier_name, s.country_code, s.region, s.is_active,
               COUNT(*) AS order_lines,
               {KPIS['order_value'][3]} AS order_value_eur,
               {KPIS['on_time_delivery_rate'][3]} AS on_time_delivery_rate,
               {KPIS['average_delay_days'][3]} AS average_delay_days,
               {KPIS['supplier_fill_rate'][3]} AS supplier_fill_rate,
               {KPIS['delayed_quantity'][3]} AS delayed_quantity,
               MAX(snap.as_of_date) AS as_of_date
        {FROM_PO_LINES}
        {where.sql()}
        GROUP BY s.supplier_id, s.supplier_name, s.country_code, s.region, s.is_active
        ORDER BY {SCORECARD_SORT[sort_by]} NULLS LAST, s.supplier_id
        LIMIT {limit}'''
    rows = await run_statement(request.app.state.client, statement, where.parameters)
    return ScorecardResponse(
        as_of_date=rows[0]['as_of_date'] if rows else None, filters=where.applied, count=len(rows),
        suppliers=[SupplierScore(**{k: v for k, v in r.items() if k != 'as_of_date'}) for r in rows],
    )


# =============================================================================
# Product stock-out exposure
# =============================================================================
EXPOSED = 'COALESCE(e.days_of_cover < e.days_until_arrival, false)'


class ProductExposure(BaseModel):
    product_id: str
    product_name: str | None
    category: str | None
    criticality: str | None
    stocking_location_id: str | None
    primary_supplier_id: str | None
    alternate_supplier_id: str | None
    has_active_alternate: bool | None
    on_hand_qty: float | None
    safety_stock_qty: float | None
    days_of_cover: float | None = Field(description='MAX(0, on_hand_qty - safety_stock_qty) / avg_daily_demand.')
    delayed_inbound_qty: float | None = Field(description='Delayed quantity of open inbound lines.')
    days_until_arrival: int | None = Field(description='Days until the earliest delayed open line arrives.')
    is_exposed: bool = Field(description='True when usable stock runs out before the delayed goods arrive.')
    exposed_value_eur: float | None


class ExposureResponse(BaseModel):
    as_of_date: str | None
    filters: dict[str, Any]
    count: int
    products: list[ProductExposure]


@app.get('/products/exposure', response_model=ExposureResponse, operation_id='get_product_exposure',
         summary='Products at risk of running out before delayed goods arrive',
         dependencies=[Depends(require_user)])
async def get_product_exposure(
    request: Request,
    only_exposed: bool = Query(True, description='Only products whose stock runs out before arrival.'),
    criticality: Criticality | None = Query(None, description='Only products with this criticality.'),
    without_active_alternate: bool = Query(False, description='Only products without an active alternate '
                                                              'supplier (candidates for a backup supplier).'),
    category: str | None = Query(None, max_length=60, description='Only products of this category.'),
    location_id: str | None = Query(None, pattern=ID_PATTERN, description='Only this stocking warehouse.'),
    supplier_id: str | None = Query(None, pattern=ID_PATTERN, description='Only this primary supplier.'),
    limit: int = Query(20, ge=1, le=300, description='Maximum number of products.'),
) -> ExposureResponse:
    """Stock-out exposure per product at the snapshot date, highest Exposed Value first. Answers when to
    switch a product to its alternate supplier, which critical products need a backup supplier and where
    to raise safety stock."""
    where = Filters(*([EXPOSED] if only_exposed else []),
                    *(['NOT COALESCE(p.has_active_alternate, false)'] if without_active_alternate else []))
    where.add('criticality', 'p.criticality = :criticality', criticality)
    where.add('category', 'p.category = :category', category)
    where.add('location_id', 'l.location_id = :location_id', location_id)
    where.add('supplier_id', 's.supplier_id = :supplier_id', supplier_id)
    where.applied.update(only_exposed=only_exposed, without_active_alternate=without_active_alternate)
    statement = f'''
        {SNAPSHOT_CTE}
        SELECT p.product_id, p.product_name, p.category, p.criticality,
               l.location_id AS stocking_location_id, s.supplier_id AS primary_supplier_id,
               p.alternate_supplier_id, p.has_active_alternate,
               e.on_hand_qty, e.safety_stock_qty, e.days_of_cover, e.delayed_inbound_qty, e.days_until_arrival,
               {EXPOSED} AS is_exposed,
               ROUND(CASE WHEN {EXPOSED} THEN e.delayed_inbound_qty * e.unit_cost * e.rate_to_eur ELSE 0 END, 2)
                 AS exposed_value_eur,
               snap.as_of_date
        {FROM_EXPOSURE}
        {where.sql()}
        ORDER BY exposed_value_eur DESC, e.delayed_inbound_qty DESC, p.product_id
        LIMIT {limit}'''
    rows = await run_statement(request.app.state.client, statement, where.parameters)
    return ExposureResponse(
        as_of_date=rows[0]['as_of_date'] if rows else None, filters=where.applied, count=len(rows),
        products=[ProductExposure(**{k: v for k, v in r.items() if k != 'as_of_date'}) for r in rows],
    )


# =============================================================================
# Delayed order lines (mitigation queue)
# =============================================================================
class DelayedLine(BaseModel):
    po_id: str
    po_line_number: int
    supplier_id: str | None
    supplier_name: str | None
    product_id: str | None
    product_name: str | None
    criticality: str | None
    delivery_location_id: str | None
    promised_delivery_date: str | None
    latest_eta_date: str | None = Field(description='Expected arrival according to the latest carrier ETA.')
    delay_days: int | None
    delayed_qty: float | None
    product_is_exposed: bool = Field(description='The product runs out of usable stock before arrival.')
    product_exposed_value_eur: float | None
    product_has_active_alternate: bool | None


class DelayedLinesResponse(BaseModel):
    as_of_date: str | None
    filters: dict[str, Any]
    count: int
    order_lines: list[DelayedLine]


@app.get('/orders/delayed', response_model=DelayedLinesResponse, operation_id='get_delayed_order_lines',
         summary='Open delayed purchase-order lines, in expedite priority order',
         dependencies=[Depends(require_user)])
async def get_delayed_order_lines(
    request: Request,
    only_exposed_products: bool = Query(False, description='Only lines of products that run out before arrival.'),
    criticality: Criticality | None = Query(None, description='Only products with this criticality.'),
    supplier_id: str | None = Query(None, pattern=ID_PATTERN, description='Only this supplier.'),
    product_id: str | None = Query(None, pattern=ID_PATTERN, description='Only this product.'),
    location_id: str | None = Query(None, pattern=ID_PATTERN, description='Only this destination warehouse.'),
    limit: int = Query(20, ge=1, le=500, description='Maximum number of order lines.'),
) -> DelayedLinesResponse:
    """Mitigation queue: open (not yet delivered) delayed purchase-order lines with expected arrival and stock
    exposure. Ordered by product Exposed Value, then CRITICAL products, then Delayed Quantity. Answers which
    delayed orders to expedite first."""
    where = Filters('NOT f.is_delivered', 'f.is_delayed', *([EXPOSED] if only_exposed_products else []))
    where.add('criticality', 'p.criticality = :criticality', criticality)
    where.add('supplier_id', 's.supplier_id = :supplier_id', supplier_id)
    where.add('product_id', 'p.product_id = :product_id', product_id)
    where.add('location_id', 'l.location_id = :location_id', location_id)
    where.applied['only_exposed_products'] = only_exposed_products
    statement = f'''
        {SNAPSHOT_CTE}
        SELECT f.po_id, f.po_line_number, s.supplier_id, s.supplier_name, p.product_id, p.product_name,
               p.criticality, l.location_id AS delivery_location_id,
               pd.date AS promised_delivery_date, eta.date AS latest_eta_date,
               f.delay_days, f.delayed_qty,
               {EXPOSED} AS product_is_exposed,
               ROUND(CASE WHEN {EXPOSED} THEN e.delayed_inbound_qty * e.unit_cost * e.rate_to_eur ELSE 0 END, 2)
                 AS product_exposed_value_eur,
               p.has_active_alternate AS product_has_active_alternate,
               snap.as_of_date
        FROM {FACT_PO_LINE} f
        JOIN {DIM_SUPPLIER} s ON f.supplier_key = s.supplier_key
        JOIN {DIM_PRODUCT} p ON f.product_key = p.product_key
        JOIN {DIM_LOCATION} l ON f.location_key = l.location_key
        JOIN {DIM_DATE} pd ON f.promised_delivery_date_key = pd.date_key
        JOIN {DIM_DATE} eta ON f.latest_eta_date_key = eta.date_key
        JOIN {FACT_EXPOSURE} e ON f.product_key = e.product_key
        CROSS JOIN snap
        {where.sql()}
        ORDER BY product_exposed_value_eur DESC, (p.criticality = 'CRITICAL') DESC, f.delayed_qty DESC,
                 f.po_id, f.po_line_number
        LIMIT {limit}'''
    rows = await run_statement(request.app.state.client, statement, where.parameters)
    return DelayedLinesResponse(
        as_of_date=rows[0]['as_of_date'] if rows else None, filters=where.applied, count=len(rows),
        order_lines=[DelayedLine(**{k: v for k, v in r.items() if k != 'as_of_date'}) for r in rows],
    )


# =============================================================================
# Supplier disruption impact (knowledge graph)
# =============================================================================
_graph_cache: dict[str, Any] = {}


def knowledge_graph():
    """The graph file, loaded once and reloaded when it is rebuilt (new modification time)."""
    try:
        mtime = KG_PATH.stat().st_mtime
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail='The knowledge graph has not been built yet.') from None
    if _graph_cache.get('key') != (str(KG_PATH), mtime):
        _graph_cache.update(key=(str(KG_PATH), mtime), graph=load_graph(KG_PATH))
    return _graph_cache['graph']


class AlternateSupplier(BaseModel):
    supplier_id: str
    supplier_name: str | None
    is_active: bool


class InFlightShipment(BaseModel):
    shipment_id: str | None
    status_code: str | None
    delivers_to: str | None = Field(description='Destination warehouse (location_id).')


class AffectedProduct(BaseModel):
    product_id: str | None
    product_name: str | None
    is_critical: bool
    alternate_suppliers: list[AlternateSupplier]
    has_usable_alternate: bool = Field(description='True when at least one alternate supplier is active.')
    shipments: list[InFlightShipment] = Field(description='Open (not delivered or cancelled) shipments.')


class DisruptionImpactResponse(BaseModel):
    filters: dict[str, Any]
    supplier_id: str
    supplier_name: str | None
    supplier_is_active: bool
    critical_only: bool
    affected_product_count: int
    uncovered_product_count: int = Field(description='Affected products without an active alternate supplier.')
    affected_products: list[AffectedProduct] = Field(description='Uncovered products first.')
    source: str


@app.get('/suppliers/disruption-impact', response_model=DisruptionImpactResponse,
         operation_id='get_supplier_disruption_impact',
         summary='What-if: impact of a supplier that stops delivering (knowledge graph)',
         responses={404: {'description': 'The supplier is not in the knowledge graph.'},
                    503: {'description': 'The knowledge graph has not been built.'}},
         dependencies=[Depends(require_user)])
async def get_supplier_disruption_impact(
    supplier_id: str = Query(..., pattern=ID_PATTERN, description='Supplier that may stop delivering, e.g. SUP-0009.'),
    critical_only: bool = Query(True, description='Only CRITICAL products.'),
) -> DisruptionImpactResponse:
    """Traverses the supply-chain knowledge graph (Supplier -SUPPLIES-> Product -ALTERNATE_SUPPLIER-> Supplier,
    Shipment -FOR_PRODUCT-> Product, Shipment -DELIVERS_TO-> Location): the products that depend on the supplier,
    who can replace it, and which open shipments of those products are in flight to which warehouse."""
    result = supplier_alternatives(knowledge_graph(), supplier_id, critical_only)
    if result.get('error') == 'NOT_FOUND':
        raise HTTPException(status_code=404, detail=f'Supplier {supplier_id} is not in the knowledge graph.')
    return DisruptionImpactResponse(filters={'supplier_id': supplier_id, 'critical_only': critical_only}, **result)


# =============================================================================
# Read-only SQL over the Gold tables (used by the assistant's SQL workers)
# =============================================================================
SQL_MAX_ROWS = 200


class GoldSqlQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')
    purpose: str = Field(min_length=3, max_length=300, description='What this query answers, in plain words.')
    sql: str = Field(min_length=10, max_length=4000,
                     description='One SELECT over the Gold tables, using unqualified table names.')


class GoldSqlResponse(BaseModel):
    purpose: str
    sql: str = Field(description='The statement as executed (qualified and row-capped).')
    as_of_date: str | None = Field(description='Snapshot date of the data (yyyy-MM-dd).')
    filters: dict[str, Any]
    columns: list[str]
    count: int
    truncated: bool = Field(description=f'True when the query returned more than {SQL_MAX_ROWS} rows.')
    rows: list[dict[str, Any]]


@app.post('/analytics/sql', response_model=GoldSqlResponse, operation_id='run_gold_sql',
          summary='Run one read-only SELECT over the Gold tables', dependencies=[Depends(require_user)],
          responses={400: {'description': 'The SQL is not allowed or failed on the warehouse.'}})
async def run_gold_sql(request: Request, body: GoldSqlQuery) -> GoldSqlResponse:
    """For questions the business endpoints do not cover. Accepts a single read-only SELECT over the Gold star
    schema only; confidential columns, SELECT * and external functions are refused and the result is capped at
    SQL_MAX_ROWS rows."""
    try:
        statement = prepare(body.sql, CATALOG, SCHEMA, SQL_MAX_ROWS)
    except UnsafeSql as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    client = request.app.state.client
    try:
        rows, snapshot = await asyncio.gather(
            run_statement(client, statement),
            run_statement(client, f'{SNAPSHOT_CTE} SELECT CAST(as_of_date AS STRING) AS as_of_date FROM snap'))
    except StatementFailed as exc:
        raise HTTPException(status_code=400, detail=f'The query failed: {exc.error_message[:300]}') from None
    return GoldSqlResponse(
        purpose=body.purpose, sql=statement, as_of_date=snapshot[0]['as_of_date'] if snapshot else None, filters={},
        columns=list(rows[0]) if rows else [], count=min(len(rows), SQL_MAX_ROWS),
        truncated=len(rows) > SQL_MAX_ROWS, rows=rows[:SQL_MAX_ROWS])


# =============================================================================
# Assistant (LangGraph multi-agent system over the endpoints above)
# =============================================================================
@app.post('/assistant/query', response_model=AssistantAnswer, operation_id='ask_assistant',
          summary='Answer a business question in natural language', dependencies=[Depends(require_user)])
async def ask_assistant(request: Request, body: AssistantQuery) -> AssistantAnswer:
    """Multi-agent assistant: routes the question, lets specialist agents call the business endpoints of this
    API (the only data access it has), composes an answer from the results and evaluates it (deterministic
    checks + LLM judge) before returning it with evidence, filters, measures, limitations and its execution trace."""
    if getattr(request.app.state, 'assistant_llm', None) is None:
        request.app.state.assistant_llm = default_llm()
    statements: list[dict[str, Any]] = []
    metrics_token = sql_metrics.set(statements)
    start = time.perf_counter()
    try:
        with track_llm_calls() as llm_calls:
            # In-process calls to this same app with the caller's own token: same validation and security,
            # no network hop.
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request.app),
                                         base_url='http://supply-chain-api',
                                         headers={'Authorization': request.headers['Authorization'],
                                                  CORRELATION_HEADER: correlation_id.get()},
                                         timeout=POLL_DEADLINE_SECONDS + 30) as tools_client:
                answer = await run_assistant(body.question, tools_client, request.app.state.assistant_llm,
                                       mode=body.mode)
    finally:
        sql_metrics.reset(metrics_token)
    answer.metrics = query_metrics(time.perf_counter() - start, statements, llm_calls.calls, len(answer.evidence))
    logger.info('assistant metrics', extra={'fields': {'mode': body.mode}
                | answer.metrics.model_dump(exclude={'sql_statements', 'llm'})})
    return answer


def query_metrics(seconds: float, statements: list[dict[str, Any]], llm_calls: list[dict[str, Any]],
                  tool_calls: int) -> QueryMetrics:
    durations = [s['duration_ms'] for s in statements]
    return QueryMetrics(
        total_ms=round(seconds * 1000, 1), sql_count=len(statements), sql_total_ms=round(sum(durations), 1),
        sql_avg_ms=round(sum(durations) / len(durations), 1) if durations else None,
        sql_max_ms=max(durations, default=None), sql_rows=sum(s['rows'] for s in statements),
        tool_calls=tool_calls, llm_calls=len(llm_calls),
        llm_total_ms=round(sum(c['duration_ms'] for c in llm_calls), 1),
        input_tokens=sum(c['input_tokens'] for c in llm_calls),
        output_tokens=sum(c['output_tokens'] for c in llm_calls),
        sql_statements=statements, llm=llm_calls)


# =============================================================================
# User interface (one static page using the endpoints above; not part of the API schema)
# =============================================================================
UI_PAGE = Path(__file__).with_name('ui') / 'index.html'


@app.get('/', include_in_schema=False)
async def user_interface() -> FileResponse:
    return FileResponse(UI_PAGE)


@app.get('/auth/config', include_in_schema=False)
async def auth_config() -> dict[str, str]:
    """Public sign-in settings for the page (MSAL.js); none of them is a secret."""
    return {'client_id': UI_CLIENT_ID, 'authority': AUTHORITY, 'scope': SCOPE}


# =============================================================================
# Health & Readiness
# =============================================================================
@app.get('/health', operation_id='get_health', summary='Liveness: the API process is running')
async def health() -> dict[str, str]:
    """Liveness probe. Does not call Databricks, so a stopped SQL warehouse does not make the API look dead."""
    return {'status': 'ok'}


@app.get('/ready', operation_id='get_readiness', summary='Readiness: the Databricks SQL warehouse answers',
         responses={503: {'description': 'Databricks is not reachable; the API cannot serve data yet.'}})
async def ready(request: Request) -> JSONResponse:
    """Readiness probe: runs SELECT 1 on the SQL warehouse. 200 = ready to serve data, 503 = not ready."""
    try:
        await run_statement(request.app.state.client, 'SELECT 1')
    except (HTTPException, httpx.HTTPError) as exc:
        logger.warning('Readiness check failed: %s', type(exc).__name__)
        return JSONResponse(status_code=503, content={'status': 'not_ready', 'databricks': 'unavailable'})
    return JSONResponse(status_code=200, content={'status': 'ready', 'databricks': 'ok'})


if __name__ == '__main__':
    import uvicorn

    uvicorn.run('app:app', host=os.environ.get('API_HOST', '127.0.0.1'),
                port=int(os.environ.get('API_PORT', '8000')))
