"""Guard for SQL written by the assistant's SQL workers (POST /analytics/sql).

Only one read-only SELECT over the Gold tables below is accepted. Tables are qualified with the configured
catalog.schema, confidential columns (business brief) and SELECT * are refused, and the result is capped.
GOLD_TABLES is also what the SQL planner is shown, so the model and the guard share one source.
"""
import sqlglot
from sqlglot import exp

# table -> (grain / purpose, allowed columns). Confidential columns are deliberately not listed.
GOLD_TABLES: dict[str, tuple[str, list[str]]] = {
    'gold_fact_purchase_order_line': (
        'one purchase-order line (cancelled lines excluded)',
        ['po_id', 'po_line_number', 'supplier_key', 'product_key', 'location_key (delivery warehouse)',
         'order_date_key', 'promised_delivery_date_key', 'actual_delivery_date_key (-1 = not delivered)',
         'latest_eta_date_key (-1 = no shipment event)', 'line_status (OPEN, PARTIALLY_RECEIVED, CLOSED)',
         'ordered_qty', 'rate_to_eur', 'received_qty', 'delay_days', 'delayed_qty', 'is_delivered (boolean)',
         'is_on_time (boolean)', 'is_delayed (boolean)']),
    'gold_fact_shipment_event': (
        'one shipment status event',
        ['event_id', 'shipment_id', 'po_id', 'shipment_status_key', 'supplier_key', 'location_key (event location)',
         'event_date_key', 'carrier', 'delay_reason_code (nullable)', 'event_ts (UTC timestamp)',
         'eta_ts (UTC timestamp)']),
    'gold_fact_inventory_exposure': (
        'one product at the snapshot date (as-of date of the data)',
        ['snapshot_date_key', 'product_key', 'supplier_key (primary supplier)', 'location_key (stocking warehouse)',
         'on_hand_qty', 'safety_stock_qty', 'rate_to_eur', 'days_of_cover', 'delayed_inbound_qty',
         'days_until_arrival']),
    'gold_dim_date': ('one calendar day; date_key = yyyyMMdd',
                      ['date_key', 'date', 'year', 'quarter', 'month', 'month_name']),
    'gold_dim_supplier': ('one supplier',
                          ['supplier_key', 'supplier_id', 'supplier_name', 'country_code', 'region (EMEA, APAC, AMER)',
                           'default_currency', 'is_active (boolean)', 'onboarded_date']),
    'gold_dim_product': ('one product',
                         ['product_key', 'product_id', 'product_name', 'category', 'unit_of_measure',
                          'criticality (CRITICAL, HIGH, MEDIUM, LOW)', 'primary_supplier_id', 'alternate_supplier_id',
                          'stocking_location_id', 'has_active_alternate (boolean)']),
    'gold_dim_location': ('one location',
                          ['location_key', 'location_id', 'location_name', 'location_country',
                           'location_type (WAREHOUSE, PORT, HUB, SUPPLIER_SITE)']),
    'gold_dim_shipment_status': ('one shipment status', ['shipment_status_key', 'status']),
}

# Confidential in the data dictionary: never readable through generated SQL (not even aggregated).
CONFIDENTIAL_COLUMNS = {'unit_price', 'unit_cost', 'avg_daily_demand', 'contract_lead_time_days', 'risk_rating',
                        'risk_level', 'risk_level_key'}
# Functions that reach outside the Gold tables (files, secrets, HTTP, JVM, AI) are refused.
BLOCKED_FUNCTION_PREFIXES = ('read_', 'reflect', 'try_reflect', 'java_', 'secret', 'try_secret', 'http_', 'ai_',
                             'vector_search', 'current_user', 'session_user', 'user', 'is_member', 'is_account_group')
WRITE_NODES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter, exp.Command,
               exp.TruncateTable, exp.Set, exp.Use)

GOLD_SCHEMA = '\n'.join(f'- {name} ({grain}): {", ".join(columns)}' for name, (grain, columns) in GOLD_TABLES.items()) + '''
Joins: facts join dimensions on *_key (e.g. f.supplier_key = s.supplier_key, f.order_date_key = d.date_key).
Every dimension has an Unknown member with key -1. Date keys are integers yyyyMMdd. Business IDs look like \
SUP-0007, PRD-00080, WH-MIL-01, PO-2025-00208.'''


class UnsafeSql(ValueError):
    """The SQL is not a single read-only query over the allowed Gold tables."""


def prepare(sql: str, catalog: str, schema: str, max_rows: int) -> str:
    """Validates `sql` and returns it qualified with catalog.schema, wrapped to return at most max_rows + 1 rows
    (the extra row tells the caller the result was truncated)."""
    try:
        statements = [s for s in sqlglot.parse(sql, read='databricks') if s is not None]
    except sqlglot.errors.SqlglotError as exc:
        raise UnsafeSql(f'The SQL could not be parsed: {str(exc).splitlines()[0][:200]}') from None
    if len(statements) != 1:
        raise UnsafeSql('Send exactly one SELECT statement.')
    tree = statements[0]
    if not isinstance(tree, exp.Query) or any(isinstance(node, WRITE_NODES) for node in tree.walk()):
        raise UnsafeSql('Only read-only SELECT queries are allowed.')

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if not isinstance(table.this, exp.Identifier):
            raise UnsafeSql('Table functions are not allowed; read only the Gold tables.')
        if name in cte_names and not table.args.get('db'):
            continue
        if name not in GOLD_TABLES:
            raise UnsafeSql(f'Table {table.sql()!r} is not allowed. Use only: {", ".join(GOLD_TABLES)}.')
        table.set('db', exp.to_identifier(schema))
        table.set('catalog', exp.to_identifier(catalog))

    for column in tree.find_all(exp.Column):
        if column.name.lower() in CONFIDENTIAL_COLUMNS:
            raise UnsafeSql(f'Column {column.name!r} is confidential and cannot be queried.')
    for star in tree.find_all(exp.Star):
        if not isinstance(star.parent, exp.Count):
            raise UnsafeSql('SELECT * is not allowed; name the columns you need.')
    for function in tree.find_all(exp.Func):
        name = (function.name if isinstance(function, exp.Anonymous) else function.sql_name()).lower()
        if name.startswith(BLOCKED_FUNCTION_PREFIXES):
            raise UnsafeSql(f'Function {name!r} is not allowed.')

    return exp.select('*').from_(tree.subquery('gold_query')).limit(max_rows + 1).sql(dialect='databricks')
