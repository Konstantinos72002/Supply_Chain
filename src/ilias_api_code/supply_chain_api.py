"""Team 4 Supply Chain Disruption API: business questions answered from the Databricks Gold layer.

Run:
    pip install fastapi uvicorn httpx python-dotenv langgraph langchain-openai "pyjwt[crypto]"
    python supply_chain_api.py

Credentials are read from a .env file next to this file (see .env.example).
Callers sign in with Microsoft Entra ID and send the access token as 'Authorization: Bearer <token>'.
User interface: http://127.0.0.1:8000/   Interactive docs: http://127.0.0.1:8000/docs
"""
import asyncio
import os
import re
import time
from contextlib import asynccontextmanager
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
from assistant.schemas import AssistantAnswer, AssistantQuery

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

STATEMENTS_PATH = '/api/2.0/sql/statements'
POLL_DEADLINE_SECONDS = 120

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


def check_response(response: httpx.Response) -> dict[str, Any]:
    if response.is_error:
        raise HTTPException(status_code=502, detail={
            'databricks_status': response.status_code, 'databricks_response': response.text})
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
    payload: dict[str, Any] = {
        'warehouse_id': WAREHOUSE_ID, 'statement': statement, 'wait_timeout': '10s',
        'disposition': 'INLINE', 'format': 'JSON_ARRAY',
    }
    if parameters:
        payload['parameters'] = parameters
    result = check_response(await client.post(STATEMENTS_PATH, json=payload))
    statement_id = result['statement_id']

    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    while result['status']['state'] in {'PENDING', 'RUNNING'}:
        if time.monotonic() >= deadline:
            try:
                await client.post(f'{STATEMENTS_PATH}/{statement_id}/cancel')
            except httpx.HTTPError:
                pass
            raise HTTPException(status_code=504, detail=f'Statement {statement_id} timed out and was cancelled.')
        await asyncio.sleep(1)
        result = check_response(await client.get(f'{STATEMENTS_PATH}/{statement_id}'))

    if result['status']['state'] != 'SUCCEEDED':
        raise HTTPException(status_code=502, detail=result['status'])

    schema = result['manifest']['schema']['columns']
    chunk = result.get('result', {})
    rows = list(chunk.get('data_array', []))
    while link := chunk.get('next_chunk_internal_link'):
        chunk = check_response(await client.get(link))
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
    # In-process calls to this same app with the caller's own token: same validation and security, no network hop.
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request.app), base_url='http://supply-chain-api',
                                 headers={'Authorization': request.headers['Authorization']},
                                 timeout=POLL_DEADLINE_SECONDS + 30) as tools_client:
        return await run_assistant(body.question, tools_client, request.app.state.assistant_llm)


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
# Health
# =============================================================================
@app.get('/health', operation_id='get_health', summary='Service and Databricks SQL warehouse health')
async def health(request: Request) -> dict[str, str]:
    await run_statement(request.app.state.client, 'SELECT 1')
    return {'status': 'ok'}


if __name__ == '__main__':
    import uvicorn

    uvicorn.run('supply_chain_api:app', host=os.environ.get('API_HOST', '127.0.0.1'),
                port=int(os.environ.get('API_PORT', '8000')))
