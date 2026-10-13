"""Guard for the text-to-SQL tool (run_sql_query).

The LLM never sees the Gold star schema (surrogate keys, date keys). It writes SQL against a few flat LOGICAL
VIEWS (products, suppliers, warehouses, po_lines) whose columns are listed in VIEWS. The SQL is accepted only if
it is one read-only SELECT over these views and columns. Confidential columns (data_dictionary.md) are never
part of a view and are blocked by name as well. The API validates again, then replaces each view name with its
defining subquery over the real Gold tables (to_physical).
"""
from collections.abc import Callable

import sqlglot
from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.qualify import qualify

MAX_ROWS = 200
DIALECT = 'databricks'

# view -> {column: description for the LLM}
VIEWS: dict[str, dict[str, str]] = {
    'products': {
        'product_id': 'e.g. PRD-00073, always 5 digits',
        'product_name': 'product description, e.g. Gearbox B34',
        'category': 'product category',
        'criticality': 'CRITICAL, HIGH, MEDIUM, LOW',
        'primary_supplier_id': 'main supplier, e.g. SUP-0037',
        'alternate_supplier_id': 'approved backup supplier, may be NULL',
        'has_active_alternate': 'true when an active alternate supplier exists',
        'stocking_location_id': 'warehouse that stocks it, e.g. WH-ATH-01',
    },
    'suppliers': {
        'supplier_id': 'e.g. SUP-0037, always 4 digits',
        'supplier_name': 'legal or trading name',
        'country_code': 'ISO 3166-1 alpha-2',
        'region': 'EMEA, APAC, AMER',
        'is_active': 'approved for ordering',
    },
    'warehouses': {
        'location_id': 'e.g. WH-ATH-01',
    },
    'po_lines': {
        'po_id': 'e.g. PO-2025-00208',
        'po_line_number': 'line within the PO',
        'supplier_id': 'supplier of the line',
        'product_id': 'product ordered',
        'delivery_location_id': 'destination warehouse',
        'order_date': 'date',
        'promised_delivery_date': 'date',
        'latest_eta_date': 'latest carrier ETA, NULL when there is no ETA',
        'ordered_qty': 'units ordered',
        'received_qty': 'units received',
        'delayed_qty': 'units not received by the promised date (plus open units with a later ETA)',
        'delay_days': 'days late',
        'is_delivered': 'boolean',
        'is_on_time': 'boolean',
        'is_delayed': 'boolean',
    },
}

# Defining SQL of each view over the Gold tables; {t[name]} = fully qualified Gold table.
VIEW_SQL: dict[str, str] = {
    'products': '''SELECT p.product_id, p.product_name, p.category, p.criticality,
                          s.supplier_id AS primary_supplier_id, p.alternate_supplier_id, p.has_active_alternate,
                          l.location_id AS stocking_location_id
                   FROM {t[gold_dim_product]} p
                   LEFT JOIN {t[gold_fact_inventory_exposure]} e ON e.product_key = p.product_key
                   LEFT JOIN {t[gold_dim_supplier]} s ON e.supplier_key = s.supplier_key
                   LEFT JOIN {t[gold_dim_location]} l ON e.location_key = l.location_key''',
    'suppliers': '''SELECT supplier_id, supplier_name, country_code, region, is_active
                    FROM {t[gold_dim_supplier]}''',
    'warehouses': '''SELECT location_id FROM {t[gold_dim_location]}''',
    'po_lines': '''SELECT f.po_id, f.po_line_number, s.supplier_id, p.product_id,
                          l.location_id AS delivery_location_id,
                          od.date AS order_date, pd.date AS promised_delivery_date, eta.date AS latest_eta_date,
                          f.ordered_qty, f.received_qty, f.delayed_qty, f.delay_days,
                          f.is_delivered, f.is_on_time, f.is_delayed
                   FROM {t[gold_fact_purchase_order_line]} f
                   JOIN {t[gold_dim_supplier]} s ON f.supplier_key = s.supplier_key
                   JOIN {t[gold_dim_product]} p ON f.product_key = p.product_key
                   JOIN {t[gold_dim_location]} l ON f.location_key = l.location_key
                   LEFT JOIN {t[gold_dim_date]} od ON f.order_date_key = od.date_key
                   LEFT JOIN {t[gold_dim_date]} pd ON f.promised_delivery_date_key = pd.date_key
                   LEFT JOIN {t[gold_dim_date]} eta ON f.latest_eta_date_key = eta.date_key''',
}

# Confidential in data_dictionary.md: blocked by name even if someone adds them to a view by mistake.
BLOCKED_COLUMNS = {'unit_cost', 'unit_price', 'avg_daily_demand', 'contract_lead_time_days', 'risk_rating'}

ALLOWED_FUNCTIONS = {
    exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Round, exp.Coalesce, exp.Lower, exp.Upper,
    exp.Cast, exp.TryCast, exp.Case, exp.If, exp.Abs, exp.Length, exp.Trim, exp.And, exp.Or, exp.Not,
    exp.CurrentDate, exp.DateDiff, exp.DateAdd, exp.Year, exp.Month, exp.DateTrunc, exp.TimestampTrunc,
}


class UnsafeSQL(ValueError):
    pass


def schema_prompt() -> str:
    """View description for the LLM."""
    return '\n'.join(f'- {view}: ' + ', '.join(f'{c} ({d})' for c, d in cols.items()) for view, cols in VIEWS.items())


def validate_sql(sql: str) -> str:
    """Returns a normalised, LIMIT-capped SELECT or raises UnsafeSQL with a message the LLM can act on."""
    try:
        statements = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise UnsafeSQL(f'SQL does not parse: {exc}') from exc
    if len(statements) != 1:
        raise UnsafeSQL('Exactly one statement is allowed.')
    tree = statements[0]
    if not isinstance(tree, exp.Select):
        raise UnsafeSQL('Only a single SELECT is allowed (no UNION, INSERT, UPDATE, DELETE, DDL).')
    if any(isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter,
                             exp.Command)) for node in tree.walk()):
        raise UnsafeSQL('Only read-only SELECT queries are allowed.')

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if table.args.get('db') or table.args.get('catalog'):
            raise UnsafeSQL(f'Use only the view names, without catalog or schema: {table.sql(dialect=DIALECT)}')
        if table.name.lower() not in VIEWS and table.name.lower() not in cte_names:
            raise UnsafeSQL(f'Unknown table {table.name}. Available views: {", ".join(VIEWS)}')

    for column in tree.find_all(exp.Column):
        if column.name.lower() in BLOCKED_COLUMNS:
            raise UnsafeSQL(f'Column {column.name} is confidential and cannot be queried.')
    for star in tree.find_all(exp.Star):
        if not isinstance(star.parent, exp.Count):
            raise UnsafeSQL('SELECT * is not allowed; name the columns you need.')
    # Resolve every column to the view it really comes from (catches tricks such as `SELECT x AS a, a AS x`).
    schema = {view: {c: 'STRING' for c in cols} for view, cols in VIEWS.items()}
    try:
        qualify(tree.copy(), schema=schema, dialect=DIALECT, validate_qualify_columns=True,
                quote_identifiers=False, identify=False)
    except (OptimizeError, sqlglot.errors.SchemaError) as exc:
        raise UnsafeSQL(f'Unknown column: {exc}. Use only the columns listed for each view.') from exc

    for func in tree.find_all(exp.Func):
        if type(func) not in ALLOWED_FUNCTIONS:
            raise UnsafeSQL(f'Function not allowed: {func.sql(dialect=DIALECT)[:40]}')

    limit = tree.args.get('limit')
    try:
        current = int(limit.expression.name) if limit is not None else None
    except (AttributeError, ValueError):
        current = None
    if current is None or current > MAX_ROWS:
        tree = tree.limit(MAX_ROWS)
    return tree.sql(dialect=DIALECT)


def to_physical(sql: str, gold_table: Callable[[str], str]) -> str:
    """API side: replaces each view name with its defining subquery over the Gold tables.
    `gold_table('gold_dim_product')` must return the fully qualified name (catalog.schema.table)."""
    names = ('gold_dim_product', 'gold_dim_supplier', 'gold_dim_location', 'gold_dim_date',
             'gold_fact_purchase_order_line', 'gold_fact_inventory_exposure')
    tables = {n: gold_table(n) for n in names}
    tree = sqlglot.parse_one(sql, read=DIALECT)
    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in list(tree.find_all(exp.Table)):
        view = table.name.lower()
        if view in VIEWS and view not in cte_names:
            body = sqlglot.parse_one(VIEW_SQL[view].format(t=tables), read=DIALECT)
            table.replace(exp.Subquery(this=body, alias=exp.TableAlias(this=exp.to_identifier(table.alias_or_name))))
    return tree.sql(dialect=DIALECT)
