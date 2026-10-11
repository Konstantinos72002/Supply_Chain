"""Read the knowledge-graph inputs from the Gold tables in Databricks.

Runs in two places:
  * inside a Databricks notebook/job -> uses the active Spark session
  * locally                          -> uses a SQL warehouse via databricks-sql-connector,
                                        configured from .env:
      DATABRICKS_HOST, DATABRICKS_TOKEN,
      DATABRICKS_HTTP_PATH or DATABRICKS_WAREHOUSE_ID,
      DATABRICKS_CATALOG (default data2026acc123456), DATABRICKS_SCHEMA (default team4)

All queries return business keys (supplier_id, product_id, ...), never the *_key surrogates.
"""
from __future__ import annotations

import logging
import os
from typing import Callable

logger = logging.getLogger(__name__)

DEFAULT_CATALOG = "data2026acc123456"
DEFAULT_SCHEMA = "team4"

Rows = list[dict]
QueryFn = Callable[[str], Rows]


def gold_queries(prefix: str) -> dict[str, str]:
    """SQL for each input of build_graph; `prefix` is '<catalog>.<schema>'."""
    return {
        "suppliers": f"""
            SELECT supplier_id, supplier_name, is_active, risk_rating
            FROM {prefix}.gold_dim_supplier
            WHERE supplier_key <> -1
        """,
        "products": f"""
            SELECT product_id, product_name, criticality,
                   criticality = 'CRITICAL' AS is_critical,
                   primary_supplier_id, alternate_supplier_id
            FROM {prefix}.gold_dim_product
            WHERE product_key <> -1
        """,
        "locations": f"""
            SELECT location_id, location_name, location_country, location_type
            FROM {prefix}.gold_dim_location
            WHERE location_key <> -1
        """,
        # Open shipments with their latest status. Gold events carry no product, so the
        # shipment -> PO line link comes from silver_shipment_events (event x PO line);
        # product and destination warehouse come from the Gold PO line.
        "shipments": f"""
            WITH latest AS (
                SELECT f.shipment_id, s.status AS status_code,
                       ROW_NUMBER() OVER (PARTITION BY f.shipment_id
                                          ORDER BY f.event_ts DESC, f.event_id DESC) AS rn
                FROM {prefix}.gold_fact_shipment_event f
                JOIN {prefix}.gold_dim_shipment_status s
                  ON f.shipment_status_key = s.shipment_status_key
            ),
            shipment_lines AS (
                SELECT DISTINCT shipment_id, po_id, po_line_number
                FROM {prefix}.silver_shipment_events
            )
            SELECT DISTINCT sl.shipment_id, p.product_id, l.location_id, lt.status_code
            FROM latest lt
            JOIN shipment_lines sl ON sl.shipment_id = lt.shipment_id
            JOIN {prefix}.gold_fact_purchase_order_line po
              ON po.po_id = sl.po_id AND po.po_line_number = sl.po_line_number
            JOIN {prefix}.gold_dim_product p
              ON p.product_key = po.product_key AND p.product_key <> -1
            LEFT JOIN {prefix}.gold_dim_location l
              ON l.location_key = po.location_key AND l.location_key <> -1
            WHERE lt.rn = 1 AND lt.status_code NOT IN ('DELIVERED', 'CANCELLED')
        """,
    }


def _spark_query_fn() -> QueryFn | None:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    spark = SparkSession.getActiveSession()
    if spark is None:
        return None
    return lambda q: [r.asDict() for r in spark.sql(q).collect()]


def _warehouse_query_fn() -> tuple[QueryFn, Callable[[], None]]:
    from databricks import sql  # databricks-sql-connector

    host = os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    if not http_path:
        http_path = f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}"

    conn = sql.connect(server_hostname=host, http_path=http_path,
                       access_token=os.environ["DATABRICKS_TOKEN"])

    def query(q: str) -> Rows:
        with conn.cursor() as cur:
            cur.execute(q)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    return query, conn.close


def fetch_gold_rows(catalog: str | None = None, schema: str | None = None) -> dict[str, Rows]:
    """Return {'suppliers', 'products', 'locations', 'shipments'} rows read from Databricks."""
    catalog = catalog or os.getenv("DATABRICKS_CATALOG", DEFAULT_CATALOG)
    schema = schema or os.getenv("DATABRICKS_SCHEMA", DEFAULT_SCHEMA)
    queries = gold_queries(f"{catalog}.{schema}")

    query, close = _spark_query_fn(), (lambda: None)
    if query is None:
        query, close = _warehouse_query_fn()
    try:
        rows = {name: query(q) for name, q in queries.items()}
    finally:
        close()

    logger.info("read from %s.%s: %s", catalog, schema,
                {k: len(v) for k, v in rows.items()})
    return rows
