"""The approved tools. Each mirrors one endpoint of the API; the assistant has no other data access.
Specialists bind the six in SPECIALIST_TOOLS; the SQL lookup path calls get_sql_schema, validate_sql_query and
run_sql_query from its own graph nodes. The API validates all SQL (read-only, Gold tables, no confidential columns,
known columns, row limit) before anything runs.

Arguments are validated with these Pydantic models before any call; the API validates them again.
"""
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

ID = r'^[A-Za-z0-9_\-]{1,40}$'
Region = Literal['EMEA', 'APAC', 'AMER']
Criticality = Literal['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
KpiName = Literal['order_value', 'on_time_delivery_rate', 'average_delay_days', 'supplier_fill_rate',
                  'exposed_value', 'delayed_quantity', 'critical_item_count', 'alternate_supplier_coverage']


class QueryKpiArgs(BaseModel):
    model_config = ConfigDict(extra='forbid')
    kpi: KpiName = Field(description='Business brief KPI.')
    group_by: Literal['none', 'month', 'region'] = Field(
        'none', description="'month' = trend by order month (purchase-order KPIs only); 'region' = supplier region.")
    supplier_id: str | None = Field(None, pattern=ID, description='e.g. SUP-0007')
    product_id: str | None = Field(None, pattern=ID, description='e.g. PRD-00080')
    region: Region | None = None
    category: str | None = Field(None, max_length=60)
    criticality: Criticality | None = None
    location_id: str | None = Field(None, pattern=ID, description='Warehouse, e.g. WH-MIL-01')
    order_date_from: date | None = Field(None, description='Purchase-order KPIs only (yyyy-mm-dd).')
    order_date_to: date | None = Field(None, description='Purchase-order KPIs only (yyyy-mm-dd).')


class SupplierScorecardArgs(BaseModel):
    model_config = ConfigDict(extra='forbid')
    sort_by: Literal['on_time_delivery_rate', 'supplier_fill_rate', 'average_delay_days', 'delayed_quantity',
                     'order_value'] = Field('on_time_delivery_rate', description='Worst first (order_value: largest).')
    region: Region | None = None
    supplier_id: str | None = Field(None, pattern=ID)
    order_date_from: date | None = None
    order_date_to: date | None = None
    limit: int = Field(10, ge=1, le=100, description='Use 100 to look up a supplier by name.')


class ProductExposureArgs(BaseModel):
    model_config = ConfigDict(extra='forbid')
    only_exposed: bool = Field(True, description='Only products that run out before delayed goods arrive.')
    criticality: Criticality | None = None
    without_active_alternate: bool = Field(False, description='Only products without an active alternate supplier.')
    category: str | None = Field(None, max_length=60)
    location_id: str | None = Field(None, pattern=ID, description='Stocking warehouse.')
    supplier_id: str | None = Field(None, pattern=ID, description='Primary supplier.')
    limit: int = Field(20, ge=1, le=300, description='Use 300 with only_exposed=false to look up a product by name.')


class DelayedOrderLinesArgs(BaseModel):
    model_config = ConfigDict(extra='forbid')
    only_exposed_products: bool = Field(False, description='Only lines of products that run out before arrival.')
    criticality: Criticality | None = None
    supplier_id: str | None = Field(None, pattern=ID)
    product_id: str | None = Field(None, pattern=ID)
    location_id: str | None = Field(None, pattern=ID, description='Destination warehouse.')
    limit: int = Field(20, ge=1, le=500)


class SupplierDisruptionImpactArgs(BaseModel):
    model_config = ConfigDict(extra='forbid')
    supplier_id: str = Field(pattern=ID, description='Supplier that may stop delivering, e.g. SUP-0009.')
    critical_only: bool = Field(True, description='Only CRITICAL products; false to include all products.')


class SqlSchemaArgs(BaseModel):
    model_config = ConfigDict(extra='forbid')


class RunSqlQueryArgs(BaseModel):
    model_config = ConfigDict(extra='forbid')
    sql: str = Field(min_length=6, max_length=4000,
                     description='One read-only SELECT (or WITH ... SELECT) on the Gold tables listed in your '
                                 'instructions; table names without catalog/schema; no SELECT *.')
    limit: int = Field(50, ge=1, le=200, description='Maximum rows returned (at most 200).')


@dataclass(frozen=True)
class ToolSpec:
    args_model: type[BaseModel]
    method: str
    path: str
    description: str


TOOLS: dict[str, ToolSpec] = {
    'query_kpi': ToolSpec(
        QueryKpiArgs, 'POST', '/analytics/query',
        'Compute one business brief KPI (Order Value, On-Time Delivery Rate, Average Delay, Supplier Fill Rate, '
        'Exposed Value, Delayed Quantity, Critical-Item Count, Alternate-Supplier Coverage), optionally filtered, '
        "grouped by order month (trend) or supplier region."),
    'get_supplier_scorecard': ToolSpec(
        SupplierScorecardArgs, 'GET', '/suppliers/scorecard',
        'Rank suppliers by On-Time Delivery Rate, Supplier Fill Rate, Average Delay, Delayed Quantity or Order Value '
        '(worst first). Returns supplier_id and supplier_name, so it also resolves supplier names to IDs.'),
    'get_product_exposure': ToolSpec(
        ProductExposureArgs, 'GET', '/products/exposure',
        'Products that run out of usable stock before delayed goods arrive (Exposed Value), with days of cover, '
        'days until arrival, criticality and alternate-supplier status. Returns product names.'),
    'get_delayed_order_lines': ToolSpec(
        DelayedOrderLinesArgs, 'GET', '/orders/delayed',
        'Open delayed purchase-order lines in expedite priority order (product Exposed Value, CRITICAL products, '
        'Delayed Quantity), with promised date, latest ETA and delayed quantity.'),
    'get_supplier_disruption_impact': ToolSpec(
        SupplierDisruptionImpactArgs, 'GET', '/suppliers/disruption-impact',
        'Knowledge-graph what-if for one supplier that stops delivering: the products that depend on it (CRITICAL '
        'only by default), their alternate suppliers and whether one is active, and the open shipments of those '
        'products with their status and destination warehouse. Uncovered products come first.'),
    'get_sql_schema': ToolSpec(
        SqlSchemaArgs, 'GET', '/analytics/sql/schema',
        'Gold tables, columns, exact code values and synonyms that the SQL endpoints accept.'),
    'validate_sql_query': ToolSpec(
        RunSqlQueryArgs, 'POST', '/analytics/sql/validate',
        'Dry run of one read-only SELECT: same rules as run_sql_query plus a LIMIT 0 run on the warehouse; returns no data.'),
    'run_sql_query': ToolSpec(
        RunSqlQueryArgs, 'POST', '/analytics/sql',
        'Run one read-only SELECT on the Gold tables for ad-hoc lookups, counts and lists that no business endpoint '
        'covers (e.g. suppliers in a region, products in a category, orders placed in a month). Returns columns, rows, '
        'row_count and truncated. Rejected queries come back with the reason; fix the SQL and retry.'),
}

SPECIALIST_TOOLS = {
    'kpi_analyst': ['query_kpi'],
    'supplier_analyst': ['get_supplier_scorecard', 'query_kpi'],
    'inventory_analyst': ['get_product_exposure', 'get_delayed_order_lines'],
    'network_analyst': ['get_supplier_disruption_impact', 'get_supplier_scorecard'],
    'sql_analyst': ['run_sql_query'],
}


def tool_definitions(names: list[str]) -> list[dict[str, Any]]:
    """OpenAI-style function definitions for bind_tools."""
    return [{'type': 'function', 'function': {'name': name, 'description': TOOLS[name].description,
                                              'parameters': TOOLS[name].args_model.model_json_schema()}}
            for name in names]


@dataclass
class ToolOutcome:
    kind: Literal['ok', 'invalid_arguments', 'unavailable']
    arguments: dict[str, Any]
    data: dict[str, Any] | None = None
    message: str = ''


async def call_tool(client: httpx.AsyncClient, name: str, raw_args: dict[str, Any] | None) -> ToolOutcome:
    """Validates the arguments, calls the API endpoint and classifies the outcome."""
    spec = TOOLS.get(name)
    if spec is None:
        return ToolOutcome('invalid_arguments', raw_args or {}, message=f'Unknown tool {name!r}.')
    try:
        validated = spec.args_model(**(raw_args or {}))
    except ValidationError as exc:
        problems = '; '.join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
        return ToolOutcome('invalid_arguments', raw_args or {}, message=f'Invalid arguments: {problems}. Fix and retry.')
    arguments = validated.model_dump(mode='json', exclude_none=True)
    try:
        if spec.method == 'POST':
            response = await client.post(spec.path, json=arguments)
        else:
            response = await client.get(spec.path, params=arguments)
    except httpx.HTTPError as exc:
        return ToolOutcome('unavailable', arguments, message=f'Data service unreachable ({type(exc).__name__}).')
    if response.status_code in (400, 404, 422):
        return ToolOutcome('invalid_arguments', arguments,
                           message=f'The API rejected the arguments: {response.json().get("detail")}. Fix and retry.')
    if response.is_error:
        return ToolOutcome('unavailable', arguments, message=f'Data service error (HTTP {response.status_code}).')
    return ToolOutcome('ok', arguments, data=response.json())
