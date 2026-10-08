import logging
import os
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any, Literal

import requests
from databricks.sdk.core import Config
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from langchain_core.runnables import RunnableLambda
from pydantic import BaseModel, Field

load_dotenv()

logger = logging.getLogger("team4_sc.api")
logging.basicConfig(level=logging.INFO, format="%(message)s")

APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
CATALOG = os.getenv("DATABRICKS_CATALOG", "data2026acc123456")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "team4")
SQL_CONSOLE_ENABLED = os.getenv("SQL_CONSOLE_ENABLED", "true").lower() == "true"
MAX_ROWS = int(os.getenv("SQL_MAX_ROWS", "5000"))

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Interactive Microsoft (Entra ID) sign-in only; personal access tokens are never used.
#   external-browser: opens the Microsoft login page in your browser (default)
#   azure-cli:        reuses the account from `az login`
ALLOWED_AUTH_TYPES = {"external-browser", "azure-cli"}


class _DatabricksAuth(requests.auth.AuthBase):
    """Attaches a fresh Databricks auth header to every request.

    The SDK refreshes OAuth tokens automatically, so long-running servers
    keep working after the access token expires.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def __call__(self, r):
        r.headers.update(self.cfg.authenticate())
        return r


def fq(table: str) -> str:
    """Fully-qualified Unity Catalog name for a table in our schema."""
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


# ---------------------------------------------------------------------------
# Databricks SQL Statement API client
# ---------------------------------------------------------------------------
class DatabricksQueryError(Exception):
    """The warehouse accepted the statement but it failed (bad SQL, missing table...)."""


class DatabricksConfigError(Exception):
    """DATABRICKS_HOST / warehouse settings are missing or the Microsoft sign-in failed."""


class DatabricksClient:
    def __init__(self) -> None:
        # Read env vars here, not at import time, so /health and the unit
        # tests still work when Databricks settings are absent (e.g. in CI).
        self.host = os.environ["DATABRICKS_HOST"].rstrip("/")
        if not self.host.startswith("http"):
            self.host = f"https://{self.host}"
        # Accept either the warehouse id or the HTTP path (/sql/1.0/warehouses/<id>).
        self.warehouse_id = (os.getenv("DATABRICKS_WAREHOUSE_ID")
                             or os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").rsplit("/", 1)[-1])
        self.api_url = f"{self.host}/api/2.0/sql/statements"
        # Auth: Microsoft sign-in as the current user. The SDK caches the OAuth
        # token on disk (~/.config/databricks-sdk-py) and refreshes it, so the
        # browser only opens on the first run or when the refresh token expires.
        auth_type = os.getenv("DATABRICKS_AUTH_TYPE", "external-browser")
        if auth_type not in ALLOWED_AUTH_TYPES:
            raise ValueError(f"DATABRICKS_AUTH_TYPE must be one of {sorted(ALLOWED_AUTH_TYPES)}, "
                             f"got '{auth_type}'.")
        if os.getenv("DATABRICKS_TOKEN"):
            logger.warning("DATABRICKS_TOKEN is set but ignored: the API signs in with Microsoft.")
        # Optional: DATABRICKS_AZURE_CLIENT_ID (or ARM_CLIENT_ID) = your own Entra ID app
        # registration; without it the login goes through the Databricks OAuth app,
        # which redirects to the Microsoft sign-in page for Azure Databricks.
        # Fail in seconds instead of the SDK's 5-minute retry window when the host is unreachable.
        cfg = Config(host=self.host, auth_type=auth_type,
                     retry_timeout_seconds=int(os.getenv("DATABRICKS_RETRY_TIMEOUT_S", "30")))
        cfg.authenticate()  # triggers the browser login now, not mid-query

        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        self.session.auth = _DatabricksAuth(cfg)

    def execute_sql(self, statement: str, parameters: dict[str, Any] | None = None,
                    row_limit: int | None = None, max_wait_s: float = 120) -> dict:
        body: dict[str, Any] = {
            "warehouse_id": self.warehouse_id,
            "catalog": CATALOG,
            "schema": SCHEMA,
            "statement": statement,
            "wait_timeout": "10s",
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        if parameters:
            # Named markers (:name) are bound server-side, never string-formatted.
            body["parameters"] = [
                {"name": k, "value": None if v is None else str(v)} for k, v in parameters.items()
            ]
        if row_limit:
            body["row_limit"] = row_limit

        response = self.session.post(self.api_url, json=body, timeout=30)
        response.raise_for_status()
        result = response.json()

        statement_id = result["statement_id"]
        deadline = time.monotonic() + max_wait_s

        while result["status"]["state"] in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                # Don't leave the query running on the warehouse.
                self.session.post(f"{self.api_url}/{statement_id}/cancel", timeout=10)
                raise TimeoutError(f"Statement {statement_id} is still running.")
            time.sleep(1)
            response = self.session.get(f"{self.api_url}/{statement_id}", timeout=30)
            response.raise_for_status()
            result = response.json()

        if result["status"]["state"] != "SUCCEEDED":
            error = result["status"].get("error", {})
            raise DatabricksQueryError(error.get("message") or result["status"]["state"])
        return result

    def query(self, statement: str, parameters: dict[str, Any] | None = None,
              row_limit: int | None = None) -> tuple[list[dict], list[dict], bool]:
        """Run a statement and return (columns, rows as dicts, truncated)."""
        result = self.execute_sql(statement, parameters, row_limit)
        manifest = result.get("manifest", {})
        columns = [{"name": c["name"], "type": c.get("type_name", "STRING")}
                   for c in manifest.get("schema", {}).get("columns", [])]

        data = list(result.get("result", {}).get("data_array") or [])
        next_link = result.get("result", {}).get("next_chunk_internal_link")
        while next_link and (row_limit is None or len(data) < row_limit):
            chunk = self.session.get(f"{self.host}{next_link}", timeout=30)
            chunk.raise_for_status()
            chunk_json = chunk.json()
            data.extend(chunk_json.get("data_array") or [])
            next_link = chunk_json.get("next_chunk_internal_link")

        rows = [{c["name"]: _convert(v, c["type"]) for c, v in zip(columns, row)} for row in data]
        return columns, rows, bool(manifest.get("truncated"))


def _convert(value: Any, type_name: str) -> Any:
    """JSON_ARRAY returns every value as a string; restore the basic SQL types."""
    if value is None:
        return None
    if type_name in {"BYTE", "SHORT", "INT", "LONG"}:
        return int(value)
    if type_name in {"FLOAT", "DOUBLE", "DECIMAL"}:
        return float(value)
    if type_name == "BOOLEAN":
        return value.lower() == "true"
    return value


_client: DatabricksClient | None = None
_client_lock = threading.Lock()


def get_databricks_client() -> DatabricksClient:
    global _client
    # The lock stops concurrent first requests from opening several browser logins.
    with _client_lock:
        if _client is None:
            try:
                _client = DatabricksClient()
            except Exception as exc:
                raise DatabricksConfigError(f"Databricks sign-in/configuration problem: {exc}") from exc
    return _client


# ---------------------------------------------------------------------------
# Auth: optional API key (enabled when API_KEY is set)
# ---------------------------------------------------------------------------
def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    expected = os.getenv("API_KEY")
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key header.")


# ---------------------------------------------------------------------------
# KPI catalogue: the ONLY SQL /analytics/query and /assistant/query can run
# ---------------------------------------------------------------------------
# Each source joins its fact to the dimensions under fixed aliases:
#   f = fact, s = gold_dim_supplier, p = gold_dim_product, l = gold_dim_location, d = gold_dim_date
def _fact_from(fact: str, date_key: str) -> str:
    return (f"{fq(fact)} f "
            f"LEFT JOIN {fq('gold_dim_supplier')} s ON f.supplier_key = s.supplier_key "
            f"LEFT JOIN {fq('gold_dim_product')} p ON f.product_key = p.product_key "
            f"LEFT JOIN {fq('gold_dim_location')} l ON f.location_key = l.location_key "
            f"LEFT JOIN {fq('gold_dim_date')} d ON f.{date_key} = d.date_key")


FACT_DIMENSIONS = {
    "supplier": "s.supplier_name",
    "region": "s.region",
    "country": "s.country_code",
    "risk_level": "s.risk_rating",
    "category": "p.category",
    "criticality": "p.criticality",
    "location": "l.location_name",
    "year": "d.year",
    "month": "date_format(d.date, 'yyyy-MM')",
}
FACT_FILTERS = {
    "supplier_id": "s.supplier_id",
    "region": "s.region",
    "country_code": "s.country_code",
    "risk_level": "s.risk_rating",
    "category": "p.category",
    "criticality": "p.criticality",
    "location_id": "l.location_id",
}
PRODUCT_DIMENSIONS = {"category": "p.category", "criticality": "p.criticality"}
PRODUCT_FILTERS = {"category": "p.category", "criticality": "p.criticality"}

PO_FROM = _fact_from("gold_fact_purchase_order_line", "order_date_key")
EXPOSURE_FROM = _fact_from("gold_fact_inventory_exposure", "snapshot_date_key")
SNAPSHOT_KEY = f"(SELECT MAX(snapshot_date_key) FROM {fq('gold_fact_inventory_exposure')})"

KPIS: dict[str, dict[str, Any]] = {
    "order_value": {
        "label": "Order Value", "unit": "EUR", "from": PO_FROM, "date_col": "d.date",
        "definition": "SUM(ordered_qty x unit_price x rate_to_eur)",
        "expr": "SUM(f.ordered_qty * f.unit_price * f.rate_to_eur)",
    },
    "on_time_delivery_rate": {
        "label": "On-Time Delivery Rate", "unit": "ratio", "from": PO_FROM, "date_col": "d.date",
        "definition": "count(lines where is_on_time) / count(lines where is_delivered)",
        "expr": "SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END) "
                "/ NULLIF(SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END), 0)",
    },
    "average_delay_days": {
        "label": "Average Delay", "unit": "days", "from": PO_FROM, "date_col": "d.date",
        "definition": "AVG(delay_days) where is_delivered AND delay_days > 0",
        "expr": "AVG(CASE WHEN f.is_delivered AND f.delay_days > 0 THEN f.delay_days END)",
    },
    "supplier_fill_rate": {
        "label": "Supplier Fill Rate", "unit": "ratio", "from": PO_FROM, "date_col": "d.date",
        "definition": "SUM(MIN(received_qty, ordered_qty)) / SUM(ordered_qty) where promised <= snapshot",
        "expr": f"SUM(CASE WHEN f.promised_delivery_date_key <= {SNAPSHOT_KEY} "
                f"THEN LEAST(f.received_qty, f.ordered_qty) END) "
                f"/ NULLIF(SUM(CASE WHEN f.promised_delivery_date_key <= {SNAPSHOT_KEY} "
                f"THEN f.ordered_qty END), 0)",
    },
    "delayed_quantity": {
        "label": "Delayed Quantity", "unit": "units", "from": PO_FROM, "date_col": "d.date",
        "definition": "SUM(delayed_qty)",
        "expr": "SUM(f.delayed_qty)",
    },
    "exposed_value": {
        "label": "Exposed Value", "unit": "EUR", "from": EXPOSURE_FROM, "date_col": "d.date",
        "definition": "SUM(delayed_inbound_qty x unit_cost x rate_to_eur) where days_of_cover < days_until_arrival",
        "expr": "SUM(CASE WHEN f.days_of_cover < f.days_until_arrival "
                "THEN f.delayed_inbound_qty * f.unit_cost * f.rate_to_eur ELSE 0 END)",
    },
    "critical_item_count": {
        "label": "Critical-Item Count", "unit": "products", "from": EXPOSURE_FROM, "date_col": "d.date",
        "definition": "DISTINCTCOUNT(product) where criticality = 'CRITICAL' AND delayed_inbound_qty > 0",
        "expr": "COUNT(DISTINCT CASE WHEN p.criticality = 'CRITICAL' AND f.delayed_inbound_qty > 0 "
                "THEN f.product_key END)",
    },
    "alternate_supplier_coverage": {
        "label": "Alternate-Supplier Coverage", "unit": "ratio",
        "from": f"{fq('gold_dim_product')} p", "date_col": None,
        "base_where": "p.product_key <> -1",
        "dimensions": PRODUCT_DIMENSIONS, "filters": PRODUCT_FILTERS,
        "definition": "count(CRITICAL products with active alternate) / count(CRITICAL products)",
        "expr": "SUM(CASE WHEN p.criticality = 'CRITICAL' AND p.has_active_alternate THEN 1 ELSE 0 END) "
                "/ NULLIF(SUM(CASE WHEN p.criticality = 'CRITICAL' THEN 1 ELSE 0 END), 0)",
    },
}

KpiName = Literal[tuple(KPIS)]  # type: ignore[valid-type]
Dimension = Literal[tuple(FACT_DIMENSIONS)]  # type: ignore[valid-type]


class AnalyticsFilters(BaseModel):
    supplier_id: list[str] | None = None
    region: list[str] | None = None
    country_code: list[str] | None = None
    risk_level: list[str] | None = None
    category: list[str] | None = None
    criticality: list[str] | None = None
    location_id: list[str] | None = None
    date_from: date | None = None
    date_to: date | None = None


class AnalyticsQueryRequest(BaseModel):
    kpi: KpiName
    filters: AnalyticsFilters = Field(default_factory=AnalyticsFilters)
    group_by: list[Dimension] = Field(default_factory=list, max_length=3)
    limit: int = Field(default=100, ge=1, le=1000)

    model_config = {"json_schema_extra": {"example": {
        "kpi": "on_time_delivery_rate",
        "filters": {"region": ["EMEA"], "date_from": "2026-01-01"},
        "group_by": ["supplier"], "limit": 20}}}


class AnalyticsQueryResponse(BaseModel):
    kpi: str
    label: str
    unit: str
    definition: str
    group_by: list[str]
    filters: dict[str, Any]
    rows: list[dict[str, Any]]


def build_kpi_sql(req: AnalyticsQueryRequest) -> tuple[str, dict[str, Any]]:
    """Assemble SQL only from catalogue fragments; user values go in as bound parameters."""
    kpi = KPIS[req.kpi]
    dimensions = kpi.get("dimensions", FACT_DIMENSIONS)
    filter_cols = kpi.get("filters", FACT_FILTERS)

    unsupported = [g for g in req.group_by if g not in dimensions]
    if unsupported:
        raise HTTPException(422, f"KPI '{req.kpi}' cannot be grouped by {unsupported}; "
                                 f"allowed: {sorted(dimensions)}")

    where, params = [kpi["base_where"]] if "base_where" in kpi else [], {}
    for name, values in req.filters.model_dump(exclude={"date_from", "date_to"}).items():
        if not values:
            continue
        if name not in filter_cols:
            raise HTTPException(422, f"KPI '{req.kpi}' cannot be filtered by '{name}'; "
                                     f"allowed: {sorted(filter_cols)}")
        markers = []
        for i, value in enumerate(values):
            params[f"{name}_{i}"] = value
            markers.append(f":{name}_{i}")
        where.append(f"{filter_cols[name]} IN ({', '.join(markers)})")

    for bound, op in (("date_from", ">="), ("date_to", "<=")):
        value = getattr(req.filters, bound)
        if value is None:
            continue
        if kpi["date_col"] is None:
            raise HTTPException(422, f"KPI '{req.kpi}' has no date to filter on.")
        params[bound] = value.isoformat()
        where.append(f"{kpi['date_col']} {op} CAST(:{bound} AS DATE)")

    select = [f"{dimensions[g]} AS {g}" for g in req.group_by] + [f"{kpi['expr']} AS value"]
    sql = f"SELECT {', '.join(select)} FROM {kpi['from']}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if req.group_by:
        positions = ", ".join(str(i + 1) for i in range(len(req.group_by)))
        sql += f" GROUP BY {positions} ORDER BY value DESC NULLS LAST"
    sql += f" LIMIT {req.limit}"
    return sql, params


def run_kpi(req: AnalyticsQueryRequest) -> AnalyticsQueryResponse:
    sql, params = build_kpi_sql(req)
    _, rows, _ = get_databricks_client().query(sql, params)
    kpi = KPIS[req.kpi]
    return AnalyticsQueryResponse(
        kpi=req.kpi, label=kpi["label"], unit=kpi["unit"], definition=kpi["definition"],
        group_by=list(req.group_by), filters=req.filters.model_dump(exclude_none=True, mode="json"),
        rows=rows,
    )


# ---------------------------------------------------------------------------
# Read-only SQL console: any SELECT against any Delta table
# ---------------------------------------------------------------------------
_READ_ONLY_START = {"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "VALUES", "TABLE"}
_WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|GRANT|REVOKE|COPY|"
    r"OPTIMIZE|VACUUM|REFRESH|RESTORE|CLONE|CALL|USE|SET|RESET|MSCK|CACHE|UNCACHE|ANALYZE)\b",
    re.IGNORECASE,
)


def _strip_literals_and_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql)
    sql = re.sub(r'"(?:[^"\\]|\\.)*"', '""', sql)
    return re.sub(r"`[^`]*`", "``", sql)


def ensure_read_only(statement: str) -> str:
    """Reject anything that is not a single read-only statement.

    This is a guard, not the security boundary: the Databricks token used by the
    API should only hold SELECT / USE privileges on the schema.
    """
    statement = statement.strip().rstrip(";").strip()
    bare = _strip_literals_and_comments(statement).strip()
    if not bare:
        raise HTTPException(422, "Empty SQL statement.")
    if ";" in bare:
        raise HTTPException(422, "Only one SQL statement per request is allowed.")
    if bare.split(None, 1)[0].upper() not in _READ_ONLY_START:
        raise HTTPException(422, f"Only read-only statements are allowed "
                                 f"({', '.join(sorted(_READ_ONLY_START))}).")
    match = _WRITE_KEYWORDS.search(bare)
    if match:
        raise HTTPException(422, f"Keyword '{match.group(1).upper()}' is not allowed in a read-only query.")
    return statement


class SqlQueryRequest(BaseModel):
    statement: str = Field(min_length=1, max_length=20_000)
    parameters: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    row_limit: int = Field(default=1000, ge=1, le=MAX_ROWS)

    model_config = {"json_schema_extra": {"example": {
        "statement": "SELECT supplier_name, region FROM gold_dim_supplier WHERE region = :region",
        "parameters": {"region": "EMEA"}, "row_limit": 100}}}


class SqlQueryResponse(BaseModel):
    columns: list[dict[str, str]]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool


class TableInfo(BaseModel):
    table: str
    layer: str


# ---------------------------------------------------------------------------
# Assistant: LangChain Core chain  question -> intent -> whitelisted KPI -> answer
# ---------------------------------------------------------------------------
class AssistantRequest(BaseModel):
    question: str = Field(min_length=3, max_length=1000)

    model_config = {"json_schema_extra": {"example": {
        "question": "What is the on-time delivery rate by supplier in EMEA?"}}}


class AssistantResponse(BaseModel):
    answer: str
    evidence: list[dict[str, Any]]
    filters: dict[str, Any]
    measure: str | None
    limitations: list[str]


_KPI_KEYWORDS: list[tuple[str, str]] = [
    (r"on[\s-]?time|otd", "on_time_delivery_rate"),
    (r"fill[\s-]?rate", "supplier_fill_rate"),
    (r"exposed|exposure|at[\s-]?risk value", "exposed_value"),
    (r"critical[\s-]?items?|critical products?", "critical_item_count"),
    (r"alternate|backup supplier|second source", "alternate_supplier_coverage"),
    (r"delayed (qty|quantity|units)|units delayed|how many units", "delayed_quantity"),
    (r"average delay|avg delay|delay days|how late|days late|delay", "average_delay_days"),
    (r"order value|spend|purchase value|value", "order_value"),
]
_GROUP_KEYWORDS: list[tuple[str, str]] = [
    (r"(by|per|for each) supplier", "supplier"), (r"(by|per) region", "region"),
    (r"(by|per) country", "country"), (r"(by|per) (risk|risk level)", "risk_level"),
    (r"(by|per) category", "category"), (r"(by|per) criticality", "criticality"),
    (r"(by|per) (location|warehouse)", "location"), (r"(by|per) month|monthly|trend", "month"),
    (r"(by|per) year|yearly|annual", "year"),
]


def _parse_question(inputs: dict) -> dict:
    q = inputs["question"].lower()
    kpi = next((k for pattern, k in _KPI_KEYWORDS if re.search(pattern, q)), None)
    group_by = [g for pattern, g in _GROUP_KEYWORDS if re.search(pattern, q)]
    filters: dict[str, Any] = {}
    regions = [r.upper() for r in ("emea", "apac", "amer") if re.search(rf"\b{r}\b", q)]
    if regions:
        filters["region"] = regions
    crit = [c.upper() for c in ("critical", "high", "medium", "low")
            if re.search(rf"\b{c}[\s-]+(criticality|priority)\b", q)]
    if crit:
        filters["criticality"] = crit
    risk = [r.upper() for r in ("high", "medium", "low") if re.search(rf"\b{r}[\s-]+risk\b", q)]
    if risk:
        filters["risk_level"] = risk
    supplier_ids = re.findall(r"\b(sup[-_]?\d+)\b", q)
    if supplier_ids:
        filters["supplier_id"] = [s.upper() for s in supplier_ids]
    years = re.findall(r"\b(20\d{2})\b", q)
    if years:
        filters["date_from"] = f"{min(years)}-01-01"
        filters["date_to"] = f"{max(years)}-12-31"
    return {**inputs, "kpi": kpi, "group_by": group_by[:3], "filters": filters}


def _run_intent(intent: dict) -> dict:
    if intent["kpi"] is None:
        return {**intent, "result": None, "notes": []}
    kpi = KPIS[intent["kpi"]]
    allowed_groups = kpi.get("dimensions", FACT_DIMENSIONS)
    allowed_filters = set(kpi.get("filters", FACT_FILTERS)) | (
        {"date_from", "date_to"} if kpi["date_col"] else set())
    notes = [f"Ignored grouping '{g}': not available for {kpi['label']}."
             for g in intent["group_by"] if g not in allowed_groups]
    notes += [f"Ignored filter '{f}': not available for {kpi['label']}."
              for f in intent["filters"] if f not in allowed_filters]
    req = AnalyticsQueryRequest(
        kpi=intent["kpi"],
        group_by=[g for g in intent["group_by"] if g in allowed_groups],
        filters=AnalyticsFilters(**{k: v for k, v in intent["filters"].items() if k in allowed_filters}),
        limit=20,
    )
    return {**intent, "result": run_kpi(req), "notes": notes}


def _format_value(value: Any, unit: str) -> str:
    if value is None:
        return "no data"
    if unit == "ratio":
        return f"{value:.1%}"
    if unit == "EUR":
        return f"EUR {value:,.2f}"
    if isinstance(value, float):
        return f"{value:,.2f} {unit}"
    return f"{value:,} {unit}"


def _compose_answer(state: dict) -> AssistantResponse:
    base_limitations = [
        "Answers come only from whitelisted Gold-layer KPIs; no free-form SQL is generated.",
        "Question understanding is keyword based; rephrase with the KPI name if the measure looks wrong.",
    ]
    result: AnalyticsQueryResponse | None = state["result"]
    if result is None:
        return AssistantResponse(
            answer="I could not map the question to a supported KPI. Try asking about: "
                   + ", ".join(k["label"] for k in KPIS.values()) + ".",
            evidence=[], filters=state["filters"], measure=None, limitations=base_limitations,
        )

    if not result.rows:
        answer = f"No data found for {result.label} with the given filters."
    elif not result.group_by:
        answer = f"{result.label} is {_format_value(result.rows[0]['value'], result.unit)}."
    else:
        top = result.rows[:5]
        parts = [f"{' / '.join(str(r[g]) for g in result.group_by)}: {_format_value(r['value'], result.unit)}"
                 for r in top]
        answer = (f"{result.label} by {', '.join(result.group_by)} (top {len(top)} of {len(result.rows)}): "
                  + "; ".join(parts) + ".")

    return AssistantResponse(
        answer=answer,
        evidence=[{"kpi": result.kpi, "definition": result.definition, "unit": result.unit,
                   "group_by": result.group_by, "rows": result.rows}],
        filters=result.filters,
        measure=result.kpi,
        limitations=base_limitations + state["notes"]
        + (["Grouped results are limited to the top 20 rows."] if result.group_by else []),
    )


assistant_chain = (RunnableLambda(_parse_question)
                   | RunnableLambda(_run_intent)
                   | RunnableLambda(_compose_answer))


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------
class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str

    model_config = {"json_schema_extra": {"example": {
        "status": "ok", "version": "0.1.0", "timestamp": "2026-10-08T09:00:00+00:00"}}}


class ReadyResponse(BaseModel):
    status: str
    checks: dict[str, str]


class AtRiskProduct(BaseModel):
    product_id: str | None
    product_name: str | None
    criticality: str | None
    supplier_name: str | None
    location_name: str | None
    delayed_inbound_qty: float | None
    days_of_cover: float | None
    days_until_arrival: float | None
    exposed_value_eur: float | None


# ---------------------------------------------------------------------------
# App, correlation-ID middleware, structured logs and error handling
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    # Sign in at startup so the browser opens when the server starts,
    # not halfway through somebody's first request.
    if os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_LOGIN_ON_STARTUP", "true").lower() == "true":
        try:
            get_databricks_client()
            logger.info("Signed in to Databricks with Microsoft.")
        except DatabricksConfigError as exc:
            logger.warning(f"Databricks sign-in at startup failed; will retry on first request. {exc}")
    yield


app = FastAPI(title="Team 4 – Supply Chain Disruption Analytics API", version=APP_VERSION,
              lifespan=lifespan)


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    request.state.correlation_id = correlation_id
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    logger.info({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "INFO",
        "correlation_id": correlation_id,
        "path": request.url.path,
        "status": response.status_code,
        "duration_ms": round((time.perf_counter() - start) * 1000, 1),
    })
    return response


def _error(request: Request, status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {
        "code": code, "message": message, "correlation_id": request.state.correlation_id}})


@app.exception_handler(DatabricksQueryError)
async def databricks_query_error(request: Request, exc: DatabricksQueryError):
    # The warehouse's own message (e.g. TABLE_OR_VIEW_NOT_FOUND) is useful to the caller.
    return _error(request, 400, "QUERY_FAILED", str(exc))


@app.exception_handler(TimeoutError)
async def databricks_timeout(request: Request, exc: TimeoutError):
    return _error(request, 504, "UPSTREAM_TIMEOUT", "The Databricks query timed out.")


@app.exception_handler(requests.RequestException)
@app.exception_handler(DatabricksConfigError)
async def databricks_unavailable(request: Request, exc: Exception):
    # Log the error type only; never echo tokens/hosts back to the caller.
    logger.warning({"correlation_id": request.state.correlation_id,
                    "check": "databricks", "error": type(exc).__name__})
    return _error(request, 502, "UPSTREAM_UNAVAILABLE", "Databricks SQL Warehouse is not reachable.")


# ---------------------------------------------------------------------------
# Endpoints: health
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["health"])
def health() -> HealthResponse:
    """Liveness: the process is up. Deliberately does NOT call Databricks."""
    return HealthResponse(
        status="ok",
        version=APP_VERSION,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/health/ready", response_model=ReadyResponse, tags=["health"],
         responses={503: {"description": "A dependency is unavailable"}})
def ready(request: Request):
    """Readiness: verifies the SQL Warehouse answers a trivial query."""
    try:
        get_databricks_client().execute_sql("SELECT 1", max_wait_s=20)
    except Exception as exc:
        logger.warning({"correlation_id": request.state.correlation_id,
                        "check": "databricks", "error": type(exc).__name__})
        return _error(request, 503, "UPSTREAM_UNAVAILABLE", "Databricks SQL Warehouse is not reachable.")
    return ReadyResponse(status="ready", checks={"databricks": "ok"})


@app.get("/auth/me", tags=["health"], dependencies=[Depends(require_api_key)])
def auth_me() -> dict[str, Any]:
    """The Microsoft account the API is signed in to Databricks with."""
    client = get_databricks_client()
    response = client.session.get(f"{client.host}/api/2.0/preview/scim/v2/Me", timeout=30)
    response.raise_for_status()
    me = response.json()
    return {"user_name": me.get("userName"), "display_name": me.get("displayName"),
            "auth_type": os.getenv("DATABRICKS_AUTH_TYPE", "external-browser")}


# ---------------------------------------------------------------------------
# Endpoints: analytics (whitelisted KPIs, never raw SQL)
# ---------------------------------------------------------------------------
@app.get("/analytics/kpis", tags=["analytics"], dependencies=[Depends(require_api_key)])
def list_kpis() -> list[dict[str, Any]]:
    """The KPI catalogue with the groupings and filters each KPI supports."""
    return [{"kpi": name, "label": k["label"], "unit": k["unit"], "definition": k["definition"],
             "group_by": sorted(k.get("dimensions", FACT_DIMENSIONS)),
             "filters": sorted(k.get("filters", FACT_FILTERS))
             + (["date_from", "date_to"] if k["date_col"] else [])}
            for name, k in KPIS.items()]


@app.post("/analytics/query", response_model=AnalyticsQueryResponse, tags=["analytics"],
          dependencies=[Depends(require_api_key)])
def analytics_query(req: AnalyticsQueryRequest) -> AnalyticsQueryResponse:
    """Return a whitelisted KPI, optionally filtered and grouped. Never accepts raw SQL."""
    return run_kpi(req)


# ---------------------------------------------------------------------------
# Endpoints: assistant (LangChain Core chain)
# ---------------------------------------------------------------------------
@app.post("/assistant/query", response_model=AssistantResponse, tags=["assistant"],
          dependencies=[Depends(require_api_key)])
def assistant_query(req: AssistantRequest) -> AssistantResponse:
    """Answer a natural-language question from the KPI catalogue, with evidence."""
    return assistant_chain.invoke({"question": req.question})


# ---------------------------------------------------------------------------
# Endpoints: domain
# ---------------------------------------------------------------------------
@app.get("/exposure/at-risk-products", response_model=list[AtRiskProduct], tags=["domain"],
         dependencies=[Depends(require_api_key)])
def at_risk_products(criticality: str | None = None, limit: int = 50) -> list[AtRiskProduct]:
    """Products whose stock runs out before the delayed inbound arrives (days_of_cover < days_until_arrival)."""
    sql = f"""
        SELECT p.product_id, p.product_name, p.criticality, s.supplier_name, l.location_name,
               f.delayed_inbound_qty, f.days_of_cover, f.days_until_arrival,
               f.delayed_inbound_qty * f.unit_cost * f.rate_to_eur AS exposed_value_eur
        FROM {EXPOSURE_FROM}
        WHERE f.days_of_cover < f.days_until_arrival
          AND (:criticality IS NULL OR p.criticality = :criticality)
        ORDER BY exposed_value_eur DESC NULLS LAST
        LIMIT {max(1, min(limit, 500))}"""
    params = {"criticality": criticality.upper() if criticality else None}
    _, rows, _ = get_databricks_client().query(sql, params)
    return [AtRiskProduct(**r) for r in rows]


# ---------------------------------------------------------------------------
# Endpoints: data explorer (any Delta table, read-only)
# ---------------------------------------------------------------------------
@app.get("/tables", response_model=list[TableInfo], tags=["sql"],
         dependencies=[Depends(require_api_key)])
def list_tables() -> list[TableInfo]:
    """All tables/views in the team schema."""
    _, rows, _ = get_databricks_client().query(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`")
    return [TableInfo(table=r["tableName"], layer=r["tableName"].split("_", 1)[0])
            for r in rows if not r.get("isTemporary")]


@app.get("/tables/{table}/schema", tags=["sql"], dependencies=[Depends(require_api_key)])
def table_schema(table: str = Path(pattern=_IDENTIFIER.pattern)) -> list[dict[str, Any]]:
    """Column names, types and comments of one table."""
    _, rows, _ = get_databricks_client().query(f"DESCRIBE TABLE {fq(table)}")
    columns = []
    for r in rows:
        if not r["col_name"] or r["col_name"].startswith("#"):
            break  # partition / clustering info follows the column list
        columns.append({"column": r["col_name"], "type": r["data_type"], "comment": r.get("comment")})
    return columns


@app.get("/tables/{table}/preview", response_model=SqlQueryResponse, tags=["sql"],
         dependencies=[Depends(require_api_key)])
def table_preview(table: str = Path(pattern=_IDENTIFIER.pattern), limit: int = 100) -> SqlQueryResponse:
    """First rows of one table."""
    limit = max(1, min(limit, MAX_ROWS))
    columns, rows, truncated = get_databricks_client().query(f"SELECT * FROM {fq(table)} LIMIT {limit}")
    return SqlQueryResponse(columns=columns, rows=rows, row_count=len(rows), truncated=truncated)


@app.post("/sql/query", response_model=SqlQueryResponse, tags=["sql"],
          dependencies=[Depends(require_api_key)])
def sql_query(req: SqlQueryRequest) -> SqlQueryResponse:
    """Run any read-only SQL statement against the Delta tables.

    Unqualified table names resolve to the team catalog/schema. Use `:name`
    markers plus `parameters` for values instead of string concatenation.
    Developer/analyst tool only: set SQL_CONSOLE_ENABLED=false in production.
    """
    if not SQL_CONSOLE_ENABLED:
        raise HTTPException(403, "The SQL console is disabled (SQL_CONSOLE_ENABLED=false).")
    statement = ensure_read_only(req.statement)
    columns, rows, truncated = get_databricks_client().query(statement, req.parameters, req.row_limit)
    truncated = truncated or len(rows) >= req.row_limit
    return SqlQueryResponse(columns=columns, rows=rows[:req.row_limit],
                            row_count=len(rows[:req.row_limit]), truncated=truncated)
@app.get("/suppliers/scorecard", response_model=SqlQueryResponse, tags=["domain"],
         dependencies=[Depends(require_api_key)])
def supplier_scorecard() -> SqlQueryResponse:
    """Order count, order value, on-time rate and average delay per supplier."""
    sql = f"""
        SELECT s.supplier_id,
               s.supplier_name,
               s.region,
               COUNT(*)                                              AS order_lines,
               ROUND(SUM(f.ordered_qty * f.unit_price * f.rate_to_eur), 2) AS order_value_eur,
               ROUND(SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END), 0), 3)
                                                                     AS on_time_rate,
               ROUND(AVG(CASE WHEN f.is_delivered AND f.delay_days > 0
                              THEN f.delay_days END), 1)             AS avg_delay_days
        FROM {fq('gold_fact_purchase_order_line')} f
        LEFT JOIN {fq('gold_dim_supplier')} s ON f.supplier_key = s.supplier_key
        GROUP BY s.supplier_id, s.supplier_name, s.region
        ORDER BY order_value_eur DESC
    """
    columns, rows, truncated = get_databricks_client().query(sql)
    print(f"Supplier scorecard query returned {len(rows)} rows (truncated={truncated})")
    return SqlQueryResponse(columns=columns, rows=rows, row_count=len(rows), truncated=truncated)