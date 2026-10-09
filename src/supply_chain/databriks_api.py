"""Team 4 Supply Chain API: health checks and a read-only SQL console over Databricks."""

import logging
import os
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import requests
from databricks.sdk.core import Config
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Path, Request
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

load_dotenv()

logger = logging.getLogger("team4_sc.api")
logging.basicConfig(level=logging.INFO, format="%(message)s")

APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
CATALOG = os.getenv("DATABRICKS_CATALOG", "data2026acc123456")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "team4")
SQL_CONSOLE_ENABLED = os.getenv("SQL_CONSOLE_ENABLED", "true").lower() == "true"
MAX_ROWS = int(os.getenv("SQL_MAX_ROWS", "5000"))

# Interactive Microsoft (Entra ID) sign-in only; personal access tokens are never used.
#   external-browser: opens the Microsoft login page in your browser (default)
#   azure-cli:        reuses the account from `az login`
ALLOWED_AUTH_TYPES = {"external-browser", "azure-cli"}

GOLD_TABLE = r"^gold_[A-Za-z0-9_]+$"  # the API only exposes gold tables


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


class _DatabricksAuth(requests.auth.AuthBase):
    """Adds a fresh auth header to every request (the SDK refreshes expired tokens)."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def __call__(self, r):
        r.headers.update(self.cfg.authenticate())
        return r


class DatabricksClient:
    def __init__(self) -> None:
        host = os.environ["DATABRICKS_HOST"].rstrip("/")
        self.host = host if host.startswith("http") else f"https://{host}"
        # Accept either the warehouse id or the HTTP path (/sql/1.0/warehouses/<id>).
        self.warehouse_id = (os.getenv("DATABRICKS_WAREHOUSE_ID")
                             or os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").rsplit("/", 1)[-1])
        self.api_url = f"{self.host}/api/2.0/sql/statements"

        auth_type = os.getenv("DATABRICKS_AUTH_TYPE", "external-browser")
        if auth_type not in ALLOWED_AUTH_TYPES:
            raise ValueError(f"DATABRICKS_AUTH_TYPE must be one of {sorted(ALLOWED_AUTH_TYPES)}.")

        # The SDK caches the OAuth token on disk, so the browser only opens on the first run.
        cfg = Config(host=self.host, auth_type=auth_type,
                     retry_timeout_seconds=int(os.getenv("DATABRICKS_RETRY_TIMEOUT_S", "30")))
        cfg.authenticate()  # sign in now, not in the middle of a query

        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        self.session.auth = _DatabricksAuth(cfg)

    def execute_sql(self, statement: str, parameters: dict[str, Any] | None = None,
                    row_limit: int | None = None, max_wait_s: float = 120) -> dict:
        """Submit a statement and wait until it finishes."""
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
            body["parameters"] = [{"name": k, "value": None if v is None else str(v)}
                                  for k, v in parameters.items()]
        if row_limit:
            body["row_limit"] = row_limit

        result = self._request("POST", self.api_url, json=body)
        statement_id = result["statement_id"]
        deadline = time.monotonic() + max_wait_s

        while result["status"]["state"] in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                self.session.post(f"{self.api_url}/{statement_id}/cancel", timeout=10)
                raise TimeoutError(f"Statement {statement_id} is still running.")
            time.sleep(1)
            result = self._request("GET", f"{self.api_url}/{statement_id}")

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

        # Large results come back in chunks; follow the links until we have enough rows.
        chunk = result.get("result", {})
        data = list(chunk.get("data_array") or [])
        while chunk.get("next_chunk_internal_link") and (row_limit is None or len(data) < row_limit):
            chunk = self._request("GET", f"{self.host}{chunk['next_chunk_internal_link']}")
            data.extend(chunk.get("data_array") or [])

        rows = [{c["name"]: _convert(v, c["type"]) for c, v in zip(columns, row)} for row in data]
        return columns, rows, bool(manifest.get("truncated"))

    def _request(self, method: str, url: str, **kwargs) -> dict:
        response = self.session.request(method, url, timeout=30, **kwargs)
        response.raise_for_status()
        return response.json()


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
    """Create the client once; the lock stops parallel requests opening several logins."""
    global _client
    with _client_lock:
        if _client is None:
            try:
                _client = DatabricksClient()
            except Exception as exc:
                raise DatabricksConfigError(f"Databricks sign-in/configuration problem: {exc}") from exc
    return _client


# Registered as a security scheme, so Swagger UI (/docs) asks for the key once
# via the "Authorize" button instead of on every endpoint.
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(x_api_key: str | None = Depends(api_key_header)) -> None:
    """Optional API key: only enforced when the API_KEY env var is set."""
    expected = os.getenv("API_KEY")
    if expected and x_api_key != expected:
        raise HTTPException(401, "Missing or invalid X-API-Key header.")


# ---------------------------------------------------------------------------
# SQL console guard: one read-only statement that only reads gold tables
# ---------------------------------------------------------------------------
# This is a guard, not the security boundary: the signed-in user's Unity
# Catalog grants decide what can really be read.
READ_ONLY_START = {"SELECT", "WITH"}
WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|GRANT|REVOKE|COPY|"
    r"OPTIMIZE|VACUUM|REFRESH|RESTORE|CLONE|CALL|USE|SET|RESET|MSCK|CACHE|UNCACHE|ANALYZE)\b",
    re.IGNORECASE,
)
TOKEN = re.compile(r"`[^`]*`|\w+|[^\s\w]")
# Keywords that end a FROM clause, so later commas no longer separate tables.
FROM_CLAUSE_END = {"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "QUALIFY", "WINDOW",
                   "UNION", "INTERSECT", "EXCEPT", "MINUS", "SELECT", "SORT", "CLUSTER", "DISTRIBUTE"}
# Functions that use FROM inside their parentheses, e.g. EXTRACT(YEAR FROM order_date).
FUNCTIONS_WITH_FROM = {"EXTRACT", "TRIM", "SUBSTRING", "SUBSTR", "POSITION", "OVERLAY"}
NON_GOLD_PREFIXES = ("bronze_", "silver_", "ops_")


def _strip_comments_and_strings(sql: str) -> str:
    """Blank out comments and quoted text so nothing inside them is checked."""
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql)
    return re.sub(r'"(?:[^"\\]|\\.)*"', '""', sql)


def ensure_read_only(statement: str) -> str:
    """Reject anything that is not a single SELECT statement."""
    statement = statement.strip().rstrip(";").strip()
    bare = re.sub(r"`[^`]*`", "``", _strip_comments_and_strings(statement)).strip()
    if not bare:
        raise HTTPException(422, "Empty SQL statement.")
    if ";" in bare:
        raise HTTPException(422, "Only one SQL statement per request is allowed.")
    if bare.split(None, 1)[0].upper() not in READ_ONLY_START:
        raise HTTPException(422, "Only SELECT queries (optionally starting with WITH) are allowed.")
    match = WRITE_KEYWORDS.search(bare)
    if match:
        raise HTTPException(422, f"Keyword '{match.group(1).upper()}' is not allowed in a read-only query.")
    return statement


def ensure_gold_only(statement: str) -> None:
    """Reject the query if it reads anything other than gold tables of our schema.

    Walks the tokens and checks every name that appears where a table can be:
    after FROM, JOIN or TABLE, and after a comma inside a FROM clause.
    """
    tokens = TOKEN.findall(_strip_comments_and_strings(statement))
    upper = [t.upper() for t in tokens]

    # CTE names ("name AS (") may be used like tables, but must not hide a non-gold table.
    ctes = {_unquote(tokens[i]) for i in range(len(tokens) - 2)
            if upper[i + 1] == "AS" and tokens[i + 2] == "("}
    for cte in ctes:
        if cte.startswith(NON_GOLD_PREFIXES):
            raise HTTPException(422, f"CTE name '{cte}' looks like a bronze/silver table; rename it.")

    in_from = [False]          # per parenthesis level: are we inside a FROM clause?
    in_function = [False]      # per parenthesis level: are we inside EXTRACT(...) & co?
    expect_table = False
    i = 0
    while i < len(tokens):
        tok, up = tokens[i], upper[i]

        if expect_table and tok != "(" and up != "LATERAL":
            expect_table = False
            if up not in {"SELECT", "WITH", "VALUES"}:  # otherwise a subquery or inline rows
                parts, i = _read_name(tokens, i)
                if i < len(tokens) and tokens[i] == "(":
                    raise HTTPException(422, f"Table functions such as '{parts[-1]}' are not allowed.")
                _check_gold_table(parts, ctes)
                continue

        if tok == "(":
            in_from.append(expect_table)  # "FROM (a JOIN b)" or "FROM (a, b)" is still a FROM list
            in_function.append(i > 0 and upper[i - 1] in FUNCTIONS_WITH_FROM)
        elif tok == ")" and len(in_from) > 1:
            in_from.pop()
            in_function.pop()
        elif up == "FROM" and not in_function[-1]:
            in_from[-1] = expect_table = True
        elif up in {"JOIN", "TABLE"} or (tok == "," and in_from[-1]):
            expect_table = True
        elif up in FROM_CLAUSE_END:
            in_from[-1] = False
        i += 1


def _unquote(identifier: str) -> str:
    return identifier.strip("`").lower()


def _read_name(tokens: list[str], i: int) -> tuple[list[str], int]:
    """Read a possibly qualified name (catalog.schema.table) starting at tokens[i]."""
    parts = []
    while i < len(tokens) and re.fullmatch(r"`[^`]*`|\w+", tokens[i]):
        parts.append(_unquote(tokens[i]))
        i += 1
        if i < len(tokens) and tokens[i] == ".":
            i += 1
        else:
            break
    if not parts:
        raise HTTPException(422, "Could not read a table name in the query.")
    return parts, i


def _check_gold_table(parts: list[str], ctes: set[str]) -> None:
    *qualifier, table = parts
    our_schema = qualifier in ([], [SCHEMA.lower()], [CATALOG.lower(), SCHEMA.lower()])
    is_gold = table.startswith("gold_") and our_schema
    if not is_gold and not (not qualifier and table in ctes):
        raise HTTPException(422, f"Only gold tables can be queried; '{'.'.join(parts)}' is not allowed.")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------
class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str


class ReadyResponse(BaseModel):
    status: str
    checks: dict[str, str]


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


# ---------------------------------------------------------------------------
# App, correlation-ID middleware and error handling
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    # Sign in at startup so the browser opens now, not during someone's first request.
    if os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_LOGIN_ON_STARTUP", "true").lower() == "true":
        try:
            get_databricks_client()
            logger.info("Signed in to Databricks with Microsoft.")
        except DatabricksConfigError as exc:
            logger.warning(f"Databricks sign-in at startup failed; will retry on first request. {exc}")
    yield


app = FastAPI(title="Team 4 – Supply Chain Disruption Analytics API", version=APP_VERSION,
              lifespan=lifespan,
              # Swagger UI remembers the key in the browser, even after a page reload.
              swagger_ui_parameters={"persistAuthorization": True})


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    request.state.correlation_id = correlation_id
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    logger.info({
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
    # Log the error type only; never echo hosts or tokens back to the caller.
    logger.warning({"correlation_id": request.state.correlation_id, "error": type(exc).__name__})
    return _error(request, 502, "UPSTREAM_UNAVAILABLE", "Databricks SQL Warehouse is not reachable.")


# ---------------------------------------------------------------------------
# Endpoints: health
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["health"])
def health() -> HealthResponse:
    """Liveness: the process is up. Deliberately does NOT call Databricks."""
    return HealthResponse(status="ok", version=APP_VERSION,
                          timestamp=datetime.now(timezone.utc).isoformat())


@app.get("/health/ready", response_model=ReadyResponse, tags=["health"],
         responses={503: {"description": "Databricks is unavailable"}})
def ready(request: Request):
    """Readiness: checks that the SQL Warehouse answers a trivial query."""
    try:
        get_databricks_client().execute_sql("SELECT 1", max_wait_s=20)
    except Exception as exc:
        logger.warning({"correlation_id": request.state.correlation_id, "error": type(exc).__name__})
        return _error(request, 503, "UPSTREAM_UNAVAILABLE", "Databricks SQL Warehouse is not reachable.")
    return ReadyResponse(status="ready", checks={"databricks": "ok"})


# ---------------------------------------------------------------------------
# Endpoints: SQL (read-only access to the gold tables)
# ---------------------------------------------------------------------------
@app.get("/tables", response_model=list[str], tags=["sql"],
         dependencies=[Depends(require_api_key)])
def list_tables() -> list[str]:
    """All gold tables/views in the team schema."""
    _, rows, _ = get_databricks_client().query(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`")
    return [r["tableName"] for r in rows
            if r["tableName"].startswith("gold_") and not r.get("isTemporary")]


@app.get("/tables/{table}/schema", tags=["sql"], dependencies=[Depends(require_api_key)])
def table_schema(table: str = Path(pattern=GOLD_TABLE)) -> list[dict[str, Any]]:
    """Column names, types and comments of one gold table."""
    _, rows, _ = get_databricks_client().query(f"DESCRIBE TABLE {fq(table)}")
    columns = []
    for r in rows:
        if not r["col_name"] or r["col_name"].startswith("#"):
            break  # partition / clustering info follows the column list
        columns.append({"column": r["col_name"], "type": r["data_type"], "comment": r.get("comment")})
    return columns


@app.get("/tables/{table}/preview", response_model=SqlQueryResponse, tags=["sql"],
         dependencies=[Depends(require_api_key)])
def table_preview(table: str = Path(pattern=GOLD_TABLE), limit: int = 100) -> SqlQueryResponse:
    """First rows of one gold table."""
    limit = max(1, min(limit, MAX_ROWS))
    columns, rows, truncated = get_databricks_client().query(f"SELECT * FROM {fq(table)} LIMIT {limit}")
    return SqlQueryResponse(columns=columns, rows=rows, row_count=len(rows), truncated=truncated)


@app.post("/sql/query", response_model=SqlQueryResponse, tags=["sql"],
          dependencies=[Depends(require_api_key)])
def sql_query(req: SqlQueryRequest) -> SqlQueryResponse:
    """Run a read-only SELECT against the gold tables.

    Unqualified table names resolve to the team catalog/schema. Use `:name`
    markers plus `parameters` for values instead of string concatenation.
    Set SQL_CONSOLE_ENABLED=false to turn this endpoint off.
    """
    if not SQL_CONSOLE_ENABLED:
        raise HTTPException(403, "The SQL console is disabled (SQL_CONSOLE_ENABLED=false).")
    statement = ensure_read_only(req.statement)
    ensure_gold_only(statement)
    columns, rows, truncated = get_databricks_client().query(statement, req.parameters, req.row_limit)
    rows = rows[:req.row_limit]
    return SqlQueryResponse(columns=columns, rows=rows, row_count=len(rows),
                            truncated=truncated or len(rows) >= req.row_limit)
