"""
Team 4 Supply Chain Disruption Analytics - ONE Lakeflow ETL pipeline, 3 of 3 source files.

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

# Gold layer - Lakeflow Declarative Pipeline (ETL pipeline) - Team 4 Supply Chain.
#
# Star schema = Team 4 assignment table (3 facts, 6 dimensions); derived fields = business_brief.md
# section 4. The 8 KPIs are DAX measures of the semantic model (definitions in src/gold/config.py).
#
# Delta layout (assignment 6.3): the two event-sized facts use liquid clustering on the keys the
# report filters on (date, supplier); dimensions and the product snapshot are tiny.

import sys
from datetime import date

import dlt

_SRC = spark.conf.get("pipeline.src_path")  # noqa: F821 - `spark` is provided by the pipeline
if _SRC not in sys.path:
    sys.path.append(_SRC)

from gold import transforms as T  # noqa: E402
from gold.docs import TABLES, document  # noqa: E402

_as_of = spark.conf.get("gold.as_of_date", "").strip()  # noqa: F821
AS_OF = date.fromisoformat(_as_of) if _as_of else None      # brief snapshot_date (None = today)
PROPS = {"quality": "gold"}


def read(name):
    return spark.read.table(name)  # noqa: F821


def gold_table(name, cluster_by=None, fail=None):
    """@dlt.table with documented comment + column comments, and invariant expectations."""
    def deco(fn):
        def wrapped():
            return document(fn(), name)
        wrapped.__name__ = fn.__name__
        kwargs = {"name": name, "comment": TABLES[name][0], "table_properties": PROPS}
        if cluster_by:
            kwargs["cluster_by"] = cluster_by
        out = dlt.expect_all_or_fail(fail)(wrapped) if fail else wrapped
        return dlt.table(**kwargs)(out)
    return deco


# ------------------------------------------------------------------ dimensions
@gold_table("gold_dim_date", fail={"key_present": "date_key IS NOT NULL"})
def gold_dim_date():
    return T.dim_date(spark, read("silver_purchase_orders"), read("silver_shipment_events"), AS_OF)  # noqa: F821


@gold_table("gold_dim_supplier", fail={"key_present": "supplier_key IS NOT NULL"})
def gold_dim_supplier():
    return T.dim_supplier(spark, read("silver_suppliers"))  # noqa: F821


@gold_table("gold_dim_product", fail={"key_present": "product_key IS NOT NULL"})
def gold_dim_product():
    return T.dim_product(spark, read("silver_products"), read("silver_suppliers"))  # noqa: F821


@gold_table("gold_dim_location", fail={"key_present": "location_key IS NOT NULL"})
def gold_dim_location():
    return T.dim_location(spark, read("silver_shipment_events"), read("silver_purchase_orders"),  # noqa: F821
                          read("silver_products"))


@gold_table("gold_dim_shipment_status", fail={"key_present": "shipment_status_key IS NOT NULL"})
def gold_dim_shipment_status():
    return T.dim_shipment_status(spark)  # noqa: F821


@gold_table("gold_dim_risk_level", fail={"key_present": "risk_level_key IS NOT NULL"})
def gold_dim_risk_level():
    return T.dim_risk_level(spark)  # noqa: F821


# ------------------------------------------------------------------ facts
@gold_table("gold_fact_purchase_order_line", cluster_by=["order_date_key", "supplier_key"],
            fail={"keys_present": "supplier_key IS NOT NULL AND product_key IS NOT NULL",
                  "not_cancelled": "line_status <> 'CANCELLED'",
                  "delayed_qty_not_negative": "delayed_qty >= 0",
                  "on_time_implies_delivered": "NOT is_on_time OR is_delivered"})
def gold_fact_purchase_order_line():
    return T.fact_purchase_order_line(read("silver_purchase_orders"), read("silver_shipment_events"),
                                      read("silver_suppliers"), AS_OF)


@gold_table("gold_fact_shipment_event", cluster_by=["event_date_key", "supplier_key"],
            fail={"keys_present": "event_id IS NOT NULL AND shipment_status_key IS NOT NULL"})
def gold_fact_shipment_event():
    return T.fact_shipment_event(read("silver_shipment_events"), read("silver_purchase_orders"))


@gold_table("gold_fact_inventory_exposure",
            fail={"key_present": "product_key IS NOT NULL",
                  "cover_not_negative": "days_of_cover IS NULL OR days_of_cover >= 0"})
def gold_fact_inventory_exposure():
    return T.fact_inventory_exposure(read("silver_purchase_orders"), read("silver_shipment_events"),
                                     read("silver_products"), read("silver_suppliers"), read("silver_fx_rates"),
                                     AS_OF)
