"""
Team 4 Supply Chain Disruption Analytics - ONE Lakeflow ETL pipeline, 2 of 3 source files.

    01_bronze.py  landing files (Volume)  ->  bronze_*            Auto Loader streaming tables
    02_silver.py  bronze_*                ->  silver_*, ops_dq_*  materialized views
    03_gold.py    silver_*                ->  gold_*              materialized views (star schema)

The three files belong to the same pipeline: Lakeflow builds one dependency graph across them,
so a single update runs raw -> Bronze -> Silver -> Gold in the right order.

Pipeline configuration (resources/pipelines.yml):
    pipeline.src_path          /Workspace/Users/<you>/team4_supply_chain/src
    bronze.landing_root        /Volumes/data2026acc123456/team4/landing
    gold.as_of_date            optional yyyy-MM-dd = brief "snapshot_date" (empty = today)
    spark.sql.session.timeZone UTC
"""

# Silver layer - Lakeflow Declarative Pipeline (ETL pipeline) - Team 4 Supply Chain.
#
# Reads the Bronze tables (01_bronze.py) and publishes validated, standardised, deduplicated and
# conformed tables as MATERIALIZED VIEWS (assignment 6.2), named after the data-dictionary datasets:
#
#     silver_suppliers, silver_products, silver_purchase_orders,
#     silver_shipment_events (one row per event x PO line, as the data dictionary specifies),
#     silver_fx_rates (business brief fx_rates seed)
#     silver_quarantine  -> invalid records with rule, reason and processing timestamp
#     ops_dq_issues      -> every rule hit (quality results measurable)

import sys
from functools import reduce

import dlt
from pyspark.sql import functions as F

_SRC = spark.conf.get("pipeline.src_path")  # noqa: F821 - `spark` is provided by the pipeline
if _SRC not in sys.path:
    sys.path.append(_SRC)

from bronze.config import SOURCES  # noqa: E402
from silver.transforms import (CHECKED_ENTITIES, SILVER_EVENT_COLS, SILVER_PO_COLS,  # noqa: E402
                               SILVER_PRODUCT_COLS, SILVER_SUPPLIER_COLS, accepted, check_products,
                               check_purchase_orders, check_shipment_event_lines, check_shipment_events,
                               check_suppliers, fx_rates_df, issues, po_line_keys, quarantined)

BRONZE = {name: cfg["target_table"] for name, cfg in SOURCES.items()}
PROPS = {"quality": "silver"}


def read(name):
    return spark.read.table(name)  # noqa: F821


# --------------------------------------------------------------------------- #
# Reference data: fx_rates seed (business_brief.md section 5)
# --------------------------------------------------------------------------- #
@dlt.table(name="silver_fx_rates", comment="fx_rates seed: rate_to_eur per (currency, valid_from), versioned with code",
           table_properties=PROPS)
def silver_fx_rates():
    return fx_rates_df(spark)  # noqa: F821


# --------------------------------------------------------------------------- #
# Checked datasets (internal, not published): typed rows + dq_issues per row
# --------------------------------------------------------------------------- #
@dlt.table(name="suppliers_checked", temporary=True)
def suppliers_checked():
    return check_suppliers(read(BRONZE["suppliers"]))


@dlt.table(name="products_checked", temporary=True)
def products_checked():
    return check_products(read(BRONZE["products"]), read("silver_suppliers"))


@dlt.table(name="purchase_orders_checked", temporary=True)
def purchase_orders_checked():
    return check_purchase_orders(read(BRONZE["purchase_orders"]), read("silver_suppliers"),
                                 read("silver_products"), read("silver_fx_rates"))


@dlt.table(name="shipment_events_checked", temporary=True)
def shipment_events_checked():
    return check_shipment_events(read(BRONZE["shipment_events"]), po_line_keys(read(BRONZE["purchase_orders"])))


@dlt.table(name="shipment_event_lines_checked", temporary=True)
def shipment_event_lines_checked():
    return check_shipment_event_lines(read("shipment_events_checked"), read("silver_products"),
                                      po_line_keys(read(BRONZE["purchase_orders"])))


# --------------------------------------------------------------------------- #
# Silver tables. expect_all_or_fail = invariants guaranteed by the rules above.
# --------------------------------------------------------------------------- #
@dlt.table(name="silver_suppliers", comment="suppliers: current snapshot, validated", table_properties=PROPS)
@dlt.expect_all_or_fail({"supplier_id_present": "supplier_id IS NOT NULL"})
def silver_suppliers():
    return accepted(read("suppliers_checked"), SILVER_SUPPLIER_COLS)


@dlt.table(name="silver_products", comment="products: current snapshot, validated", table_properties=PROPS)
@dlt.expect_all_or_fail({"product_id_present": "product_id IS NOT NULL"})
def silver_products():
    return accepted(read("products_checked"), SILVER_PRODUCT_COLS)


@dlt.table(name="silver_purchase_orders",
           comment="purchase_orders: one row per PO line, latest last_modified_ts wins, rate_to_eur on order_date",
           table_properties=PROPS)
@dlt.expect_all_or_fail({"key_present": "po_id IS NOT NULL AND po_line_number IS NOT NULL",
                         "ordered_qty_positive": "ordered_qty > 0",
                         "rate_present": "rate_to_eur IS NOT NULL"})
def silver_purchase_orders():
    return accepted(read("purchase_orders_checked"), SILVER_PO_COLS)


@dlt.table(name="silver_shipment_events",
           comment="shipment_events flattened to one row per event x PO line; timestamps in UTC",
           table_properties=PROPS)
@dlt.expect_all_or_fail({"key_present": "event_id IS NOT NULL AND po_line_number IS NOT NULL",
                         "event_ts_present": "event_ts IS NOT NULL",
                         "quantity_not_negative": "quantity >= 0"})
def silver_shipment_events():
    return accepted(read("shipment_event_lines_checked"), SILVER_EVENT_COLS, extra_filter=~F.col("_event_blocked"))


# --------------------------------------------------------------------------- #
# Quarantine and DQ log
# --------------------------------------------------------------------------- #
CHECKED_VIEWS = ["suppliers_checked", "products_checked", "purchase_orders_checked",
                 "shipment_events_checked", "shipment_event_lines_checked"]


def _checked(view):
    df = read(view)
    # lines of a quarantined event are already covered by the event-level quarantine row
    return df.where(~F.col("_event_blocked")) if view == "shipment_event_lines_checked" else df


@dlt.table(name="silver_quarantine",
           comment="Invalid records: one row per blocking rule, with reason, raw payload and processing timestamp",
           table_properties=PROPS)
def silver_quarantine():
    return reduce(lambda a, b: a.unionByName(b),
                  [quarantined(_checked(v), entity, key) for v, (entity, key) in zip(CHECKED_VIEWS, CHECKED_ENTITIES)])


@dlt.table(name="ops_dq_issues", comment="Every DQ rule hit (QUARANTINE, NULLIFY, FIX, FLAG) with file and line")
def ops_dq_issues():
    return reduce(lambda a, b: a.unionByName(b),
                  [issues(read(v), entity, key) for v, (entity, key) in zip(CHECKED_VIEWS, CHECKED_ENTITIES)])
