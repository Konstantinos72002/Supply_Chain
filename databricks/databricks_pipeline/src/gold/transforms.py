"""
Gold transformations - Team 4 assignment table + business_brief.md section 4.
Pure PySpark, unit-tested, used by pipelines/medallion/03_gold.py.

    gold_fact_purchase_order_line  grain: one purchase-order line (cancelled lines excluded, brief §4)
    gold_fact_shipment_event       grain: one shipment status event
    gold_fact_inventory_exposure   grain: one product at snapshot_date
    gold_dim_date, gold_dim_supplier, gold_dim_product, gold_dim_location,
    gold_dim_shipment_status, gold_dim_risk_level

Surrogate keys = xxhash64(natural key): deterministic across refreshes; -1 = Unknown member.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from gold.config import RISK_LEVELS, SHIPMENT_STATUSES, UNKNOWN_KEY

KEYS = ["po_id", "po_line_number"]


def sk(*cols) -> Column:
    cols = [F.col(c) if isinstance(c, str) else c for c in cols]
    return F.when(F.coalesce(*cols).isNull(), F.lit(UNKNOWN_KEY).cast("bigint")) \
            .otherwise(F.xxhash64(F.concat_ws("||", *[c.cast("string") for c in cols])))


def date_key(c) -> Column:
    c = F.col(c) if isinstance(c, str) else c
    return F.coalesce(F.date_format(c, "yyyyMMdd").cast("int"), F.lit(UNKNOWN_KEY))


def snapshot(as_of: date | None) -> Column:
    return F.lit(as_of) if as_of else F.current_date()


def with_unknown(spark: SparkSession, df: DataFrame, key: str, labels: dict) -> DataFrame:
    unknown = spark.range(1).select(F.lit(UNKNOWN_KEY).cast("bigint").alias(key),
                                    *[F.lit(v).alias(k) for k, v in labels.items()])
    return df.unionByName(unknown, allowMissingColumns=True)


def rate_on(df: DataFrame, fx: DataFrame, cur_col: str, on: Column) -> DataFrame:
    """rate_to_eur valid on a date: latest valid_from <= date (fx_rates seed)."""
    w = Window.partitionBy("currency").orderBy("valid_from")
    r = (fx.withColumn("_to", F.lead("valid_from").over(w))
           .select(F.col("currency").alias("_cur"), F.col("valid_from").alias("_from"), "_to", "rate_to_eur"))
    cond = (df[cur_col] == r["_cur"]) & (on >= r["_from"]) & (r["_to"].isNull() | (on < r["_to"]))
    return df.join(F.broadcast(r), cond, "left").drop("_cur", "_from", "_to")


# =========================================================================== #
# Brief section 4 - derived fields per PO line
# =========================================================================== #
def po_line_kpis(po: DataFrame, events: DataFrame, as_of: date | None) -> DataFrame:
    """Non-cancelled Silver PO lines + brief derived fields. `events` = silver_shipment_events
    (one row per event x PO line)."""
    snap = snapshot(as_of)
    po = po.where(F.col("line_status") != "CANCELLED")                 # "Cancelled lines are excluded"
    ev = (events.join(po.select(*KEYS, "promised_delivery_date"), KEYS, "inner")
                .withColumn("_deliv", F.col("status") == "DELIVERED")
                .withColumn("_date", F.to_date("event_ts")))
    agg = ev.groupBy(*KEYS).agg(
        F.sum(F.when(F.col("_deliv"), F.col("quantity"))).alias("received_qty"),
        F.sum(F.when(F.col("_deliv") & (F.col("_date") <= F.col("promised_delivery_date")),
                     F.col("quantity"))).alias("_received_by_promised"),
        F.max(F.when(F.col("_deliv"), F.col("_date"))).alias("actual_delivery_date"),
        F.max_by(F.to_date("eta_ts"), "event_ts").alias("latest_eta_date"))
    ordered, prom = F.col("ordered_qty"), F.col("promised_delivery_date")
    return (po.join(agg, KEYS, "left")
            .withColumn("received_qty", F.coalesce("received_qty", F.lit(0)).cast("decimal(18,2)"))
            .withColumn("_rbp", F.coalesce("_received_by_promised", F.lit(0)))
            .withColumn("_past_due", prom <= snap)
            .withColumn("_late_eta", F.coalesce(F.col("latest_eta_date") > prom, F.lit(False)))
            .withColumn("is_delivered", (F.col("received_qty") >= ordered) | (F.col("line_status") == "CLOSED"))
            .withColumn("delay_days",
                        F.when(F.col("is_delivered"), F.datediff("actual_delivery_date", prom))
                         .when(F.col("_past_due"), F.datediff(snap, prom))
                         .otherwise(F.lit(0)))
            .withColumn("is_on_time", F.col("is_delivered")
                        & F.coalesce(F.col("actual_delivery_date") <= prom, F.lit(False)))
            .withColumn("is_delayed",
                        (F.col("is_delivered") & F.coalesce(F.col("delay_days") > 0, F.lit(False)))
                        | (~F.col("is_delivered") & (F.col("_past_due") | F.col("_late_eta"))))
            .withColumn("delayed_qty",
                        F.when(F.col("_past_due"), F.greatest(ordered - F.col("_rbp"), F.lit(0)))
                         .when(F.col("_late_eta"), F.greatest(ordered - F.col("received_qty"), F.lit(0)))
                         .otherwise(F.lit(0)).cast("decimal(18,2)")))


def product_exposure(lines: DataFrame, products: DataFrame, suppliers: DataFrame, fx: DataFrame,
                     as_of: date | None) -> DataFrame:
    snap = snapshot(as_of)
    open_delayed = lines.where(~F.col("is_delivered") & F.col("is_delayed"))
    inbound = open_delayed.groupBy("product_id").agg(F.sum("delayed_qty").alias("delayed_inbound_qty"))
    # "earliest delayed open line" = earliest promised_delivery_date (assumption, see docs)
    w = Window.partitionBy("product_id").orderBy("promised_delivery_date", "po_id", "po_line_number")
    lead = suppliers.select("supplier_id", "contract_lead_time_days")
    earliest = (open_delayed.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
                .join(lead, "supplier_id", "left")
                .select("product_id", F.lit(True).alias("_has_delayed"),
                        F.col("latest_eta_date").alias("_eta"), F.col("delay_days").alias("_delay"),
                        F.col("contract_lead_time_days").alias("_lead")))
    demand = F.col("avg_daily_demand")
    df = (products.join(inbound, "product_id", "left").join(earliest, "product_id", "left")
          .withColumn("delayed_inbound_qty", F.coalesce("delayed_inbound_qty", F.lit(0)).cast("decimal(18,2)"))
          # NULL stock figures -> cover unknown (greatest() would skip the NULL and return 0)
          .withColumn("days_of_cover", F.when((demand > 0) & F.col("on_hand_qty").isNotNull()
                                              & F.col("safety_stock_qty").isNotNull(), F.round(
              F.greatest(F.col("on_hand_qty") - F.col("safety_stock_qty"), F.lit(0)) / demand, 2)).cast("double"))
          .withColumn("days_until_arrival",
                      F.when(F.col("_has_delayed"),
                             F.coalesce(F.datediff("_eta", snap), F.col("_delay") + F.col("_lead")))))
    return rate_on(df, fx, "cost_currency", snap)                     # unit_cost rate: snapshot date (assumption)


def has_active_alternate(products: DataFrame, suppliers: DataFrame) -> DataFrame:
    alt = suppliers.select(F.col("supplier_id").alias("alternate_supplier_id"), F.col("is_active").alias("_act"))
    return (products.join(alt, "alternate_supplier_id", "left")
            .withColumn("has_active_alternate", F.col("alternate_supplier_id").isNotNull()
                        & (F.col("alternate_supplier_id") != F.col("primary_supplier_id"))
                        & F.coalesce(F.col("_act"), F.lit(False)))
            .drop("_act"))


# =========================================================================== #
# Dimensions
# =========================================================================== #
def dim_date(spark: SparkSession, po: DataFrame, events: DataFrame, as_of: date | None) -> DataFrame:
    b = (po.select(F.col("order_date").alias("d"))
         .unionByName(po.select(F.col("promised_delivery_date").alias("d")))
         .unionByName(events.select(F.to_date("event_ts").alias("d")))
         .unionByName(events.select(F.to_date("eta_ts").alias("d")))
         .unionByName(spark.range(1).select(snapshot(as_of).alias("d")))
         .agg(F.min("d").alias("lo"), F.max("d").alias("hi")).first())
    d = spark.range(1).select(F.explode(F.sequence(F.lit(date(b.lo.year, 1, 1)),
                                                   F.lit(date(b.hi.year, 12, 31)))).alias("date"))
    d = d.select(date_key("date").alias("date_key"), "date", F.year("date").alias("year"),
                 F.quarter("date").alias("quarter"), F.month("date").alias("month"),
                 F.date_format("date", "MMMM").alias("month_name"))
    return d.unionByName(spark.range(1).select(F.lit(UNKNOWN_KEY).alias("date_key"),
                                               F.lit("Unknown").alias("month_name")), allowMissingColumns=True)


def dim_supplier(spark: SparkSession, suppliers: DataFrame) -> DataFrame:
    df = suppliers.select(sk("supplier_id").alias("supplier_key"), "supplier_id", "supplier_name", "country_code",
                          "region", "default_currency", "contract_lead_time_days", "risk_rating", "is_active",
                          "onboarded_date")
    return with_unknown(spark, df, "supplier_key", {"supplier_id": "UNKNOWN", "supplier_name": "Unknown supplier"})


def dim_product(spark: SparkSession, products: DataFrame, suppliers: DataFrame) -> DataFrame:
    df = has_active_alternate(products, suppliers).select(
        sk("product_id").alias("product_key"), "product_id", "product_name", "category", "unit_of_measure",
        "criticality", "primary_supplier_id", "alternate_supplier_id", "stocking_location_id",
        "has_active_alternate")
    return with_unknown(spark, df, "product_key", {"product_id": "UNKNOWN", "product_name": "Unknown product"})


def dim_location(spark: SparkSession, events: DataFrame, po: DataFrame, products: DataFrame) -> DataFrame:
    """Locations of shipment events, plus warehouses referenced only by PO lines / products."""
    w = Window.partitionBy("location_id").orderBy(F.col("event_ts").desc())
    seen = (events.where(F.col("location_id").isNotNull()).withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1").select("location_id", "location_name", "location_country", "location_type"))
    referenced = (po.select(F.col("delivery_location_id").alias("location_id"))
                  .unionByName(products.select(F.col("stocking_location_id").alias("location_id")))
                  .where(F.col("location_id").isNotNull()).distinct()
                  .join(seen.select("location_id"), "location_id", "left_anti"))
    df = (seen.unionByName(referenced, allowMissingColumns=True)
          .select(sk("location_id").alias("location_key"), "location_id", "location_name",
                  "location_country", "location_type"))
    return with_unknown(spark, df, "location_key", {"location_id": "UNKNOWN", "location_name": "Unknown location"})


def dim_shipment_status(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame([(s,) for s in SHIPMENT_STATUSES], "status STRING")
    return with_unknown(spark, df.select(sk("status").alias("shipment_status_key"), "status"),
                        "shipment_status_key", {"status": "UNKNOWN"})


def dim_risk_level(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame([(r,) for r in RISK_LEVELS], "risk_level STRING")
    return with_unknown(spark, df.select(sk("risk_level").alias("risk_level_key"), "risk_level"),
                        "risk_level_key", {"risk_level": "UNKNOWN"})


# =========================================================================== #
# Facts
# =========================================================================== #
def _supplier_risk(suppliers: DataFrame) -> DataFrame:
    return suppliers.select(F.col("supplier_id").alias("_rs_id"), F.col("risk_rating").alias("_risk"))


def fact_purchase_order_line(po: DataFrame, events: DataFrame, suppliers: DataFrame,
                             as_of: date | None) -> DataFrame:
    lines = po_line_kpis(po, events, as_of)
    risk = _supplier_risk(suppliers)
    lines = lines.join(risk, lines["supplier_id"] == risk["_rs_id"], "left")
    return lines.select(
        "po_id", "po_line_number",
        sk("supplier_id").alias("supplier_key"), sk("product_id").alias("product_key"),
        sk("delivery_location_id").alias("location_key"), sk("_risk").alias("risk_level_key"),
        date_key("order_date").alias("order_date_key"),
        date_key("promised_delivery_date").alias("promised_delivery_date_key"),
        date_key("actual_delivery_date").alias("actual_delivery_date_key"),
        date_key("latest_eta_date").alias("latest_eta_date_key"),
        "line_status", "ordered_qty", "unit_price", "rate_to_eur",
        "received_qty", "delay_days", "delayed_qty", "is_delivered", "is_on_time", "is_delayed")


def fact_shipment_event(events: DataFrame, po: DataFrame) -> DataFrame:
    """Silver has one row per event x PO line; the fact grain is one status event."""
    supplier = po.groupBy("po_id").agg(F.min("supplier_id").alias("supplier_id"))
    ev = events.select("event_id", "shipment_id", "po_id", "carrier", "status", "event_ts", "eta_ts",
                       "location_id", "delay_reason_code").dropDuplicates(["event_id"])
    return (ev.join(supplier, "po_id", "left")
            .select("event_id", "shipment_id", "po_id",
                    sk("status").alias("shipment_status_key"), sk("supplier_id").alias("supplier_key"),
                    sk("location_id").alias("location_key"), date_key(F.to_date("event_ts")).alias("event_date_key"),
                    "carrier", "delay_reason_code", "event_ts", "eta_ts"))


def fact_inventory_exposure(po: DataFrame, events: DataFrame, products: DataFrame, suppliers: DataFrame,
                            fx: DataFrame, as_of: date | None) -> DataFrame:
    lines = po_line_kpis(po, events, as_of)
    df = product_exposure(lines, products, suppliers, fx, as_of)
    risk = _supplier_risk(suppliers)
    df = df.join(risk, df["primary_supplier_id"] == risk["_rs_id"], "left")
    return df.select(
        date_key(snapshot(as_of)).alias("snapshot_date_key"),
        sk("product_id").alias("product_key"), sk("primary_supplier_id").alias("supplier_key"),
        sk("stocking_location_id").alias("location_key"), sk("_risk").alias("risk_level_key"),
        "on_hand_qty", "safety_stock_qty", "avg_daily_demand", "unit_cost", "rate_to_eur",
        "days_of_cover", "delayed_inbound_qty", "days_until_arrival")
