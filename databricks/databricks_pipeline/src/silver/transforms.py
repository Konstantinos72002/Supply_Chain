"""
Silver transformations - pure PySpark, unit-tested locally, used unchanged by
pipelines/medallion/02_silver.py.

Pattern for every entity
------------------------
    bronze rows -> scope (latest snapshot / all deliveries)
                -> standardise + type  (try_cast: bad values become NULL, never fail)
                -> checks              (each failed check appends a struct to `dq_issues`)
                -> dedup / FK / cross-record checks
    "checked" DataFrame  ->  accepted rows  (silver_<entity>)
                         ->  blocked rows   (silver_quarantine)
                         ->  every issue    (ops_dq_issues)

An issue is struct<rule, action, column_name, description>;
`action` = QUARANTINE | NULLIFY | FIX | FLAG (see silver.reference).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from silver.reference import (CRITICALITY, DELAY_REASON_CODES, EVENT_STATUSES, FIX,
                              FLAG, FX_RATES, ISO_CURRENCIES,
                              LOCATION_TYPES, NULLIFY, PO_LINE_STATUSES,
                              PRODUCT_CATEGORIES, QUARANTINE, REGIONS, RISK_RATINGS,
                              TIMESTAMP_FORMAT, UNITS_OF_MEASURE)

LINEAGE = ["source_file_name", "source_line_number", "batch_id"]   # traceability to Bronze


# =========================================================================== #
# Generic helpers
# =========================================================================== #
def issue(cond: Column, rule: str, action: str, column: str, description) -> Column:
    desc = description if isinstance(description, Column) else F.lit(description)
    return F.when(cond, F.struct(
        F.lit(rule).alias("rule"),
        F.lit(action).alias("action"),
        F.lit(column).alias("column_name"),
        desc.cast("string").alias("description"),
    ))


def add_issues(df: DataFrame, *issues: Column) -> DataFrame:
    new = F.filter(F.array(*issues), lambda x: x.isNotNull())
    if "dq_issues" not in df.columns:
        return df.withColumn("dq_issues", new)
    return df.withColumn("dq_issues", F.concat("dq_issues", new))


def is_blocked(col: str = "dq_issues") -> Column:
    return F.exists(col, lambda i: i["action"] == QUARANTINE)


def raw(c: str) -> Column:
    return F.col(f"{c}__raw")


def clean(c: str) -> Column:
    """Trimmed raw string, empty -> NULL."""
    v = F.trim(raw(c))
    return F.when(v != "", v)


def try_cast(c: str, sql_type: str) -> Column:
    return F.expr(f"try_cast(trim(`{c}__raw`) AS {sql_type})")


def type_issue(c: str, typed: Column, sql_type: str, action: str) -> Column:
    return issue(clean(c).isNotNull() & typed.isNull(), "invalid_type", action, c,
                 F.concat(F.lit(f"{c} '"), raw(c), F.lit(f"' is not a valid {sql_type}")))


def required_issue(c: str, action: str = QUARANTINE) -> Column:
    return issue(clean(c).isNull(), "missing_required", action, c, f"{c} is empty")


def domain_issue(c: str, typed: Column, allowed: list, action: str = FLAG) -> Column:
    return issue(typed.isNotNull() & ~typed.isin(allowed), "invalid_domain", action, c,
                 F.concat(F.lit(f"{c} '"), typed, F.lit("' not in allowed values")))


def std_currency(c: str) -> Column:
    up = F.upper(clean(c))
    return F.when(up.isin(ISO_CURRENCIES), up)


def currency_issues(c: str, invalid_action: str) -> list[Column]:
    v, up = clean(c), F.upper(clean(c))
    return [
        issue(v.isNotNull() & up.isin(ISO_CURRENCIES) & (v != up),
              "currency_inconsistency", FIX, c,
              F.concat(F.lit(f"{c} '"), v, F.lit("' normalised to '"), up, F.lit("'"))),
        issue(v.isNotNull() & ~up.isin(ISO_CURRENCIES),
              "currency_inconsistency", invalid_action, c,
              F.concat(F.lit(f"{c} '"), v, F.lit("' is not a valid ISO 4217 code"))),
    ]


def bronze_issues(df: DataFrame) -> DataFrame:
    return add_issues(
        df,
        issue(F.col("load_status") == "MALFORMED", "malformed_record", QUARANTINE, "*",
              "Row could not be parsed in Bronze"),
        issue(F.col("load_status") == "SCHEMA_DRIFT", "schema_drift", FLAG, "_rescued_data",
              F.concat(F.lit("Schema drift: "), F.col("_rescued_data"))),
    )


def prepare(bronze: DataFrame, business_cols: list[str]) -> DataFrame:
    """Rename Bronze business columns to <col>__raw and keep lineage."""
    return bronze.select(
        *[F.col(c).alias(f"{c}__raw") for c in business_cols],
        "load_status", "_rescued_data", "raw_payload",
        "source_file_name", "source_line_number", "batch_id", "record_hash",
    )


def latest_snapshot(bronze: DataFrame) -> DataFrame:
    """Full-snapshot sources: keep only the most recent delivered file."""
    # batch_id = <source>-<file modification time yyyyMMddTHHmmss>-<hash>: sorts chronologically
    latest = bronze.select("batch_id").distinct().orderBy(F.desc("batch_id")).limit(1)
    return bronze.join(F.broadcast(latest), "batch_id")


def flag_duplicates(df: DataFrame, keys: list[str], rule: str, label: str,
                    order: list[Column]) -> DataFrame:
    """All but the first occurrence of a key get a QUARANTINE issue."""
    w = Window.partitionBy(*keys).orderBy(*order)
    wf = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    key_txt = F.concat_ws(", ", *[F.col(k).cast("string") for k in keys])
    df = (df.withColumn("_rn", F.row_number().over(w))
            .withColumn("_first_line", F.first("source_line_number").over(wf))
            .withColumn("_first_hash", F.first("record_hash").over(wf)))
    desc = F.concat(F.lit(f"Duplicate {label} ("), key_txt, F.lit(") - first occurrence on line "),
                    F.col("_first_line").cast("string"),
                    F.when(F.col("record_hash") == F.col("_first_hash"), F.lit(", exact copy"))
                     .otherwise(F.lit(", conflicting attributes")))
    df = add_issues(df, issue(F.col("_rn") > 1, rule, QUARANTINE, ",".join(keys), desc))
    return df.drop("_rn", "_first_line", "_first_hash")


def key_exists(df: DataFrame, col: str, ref: DataFrame, ref_col: str, flag: str) -> DataFrame:
    keys = ref.select(F.col(ref_col).alias(f"_ref_{flag}")).where(F.col(f"_ref_{flag}").isNotNull()).distinct()
    return (df.join(F.broadcast(keys), df[col] == keys[f"_ref_{flag}"], "left")
              .withColumn(flag, F.col(f"_ref_{flag}").isNotNull())
              .drop(f"_ref_{flag}"))


def fx_rates_df(spark: SparkSession) -> DataFrame:
    return (spark.createDataFrame(FX_RATES, "currency STRING, valid_from DATE, rate_to_eur DOUBLE")
            .withColumn("rate_to_eur", F.col("rate_to_eur").cast("decimal(18,8)")))


def with_fx(df: DataFrame, rates: DataFrame, cur_col: str, date_col: Column) -> DataFrame:
    """Adds rate_to_eur valid on date_col: the rate with the latest valid_from <= date."""
    w = Window.partitionBy("currency").orderBy("valid_from")
    r = (rates.withColumn("_fx_to", F.lead("valid_from").over(w))
              .select(F.col("currency").alias("_fx_cur"), F.col("valid_from").alias("_fx_from"), "_fx_to",
                      "rate_to_eur"))
    cond = ((df[cur_col] == r["_fx_cur"]) & (date_col >= r["_fx_from"])
            & (r["_fx_to"].isNull() | (date_col < r["_fx_to"])))
    return df.join(F.broadcast(r), cond, "left").drop("_fx_cur", "_fx_from", "_fx_to")


# =========================================================================== #
# Suppliers  (full snapshot -> latest file)
# =========================================================================== #
SUPPLIER_COLS = ["supplier_id", "supplier_name", "country_code", "region", "default_currency",
                 "contract_lead_time_days", "risk_rating", "is_active", "onboarded_date"]


def check_suppliers(bronze: DataFrame) -> DataFrame:
    df = prepare(latest_snapshot(bronze), SUPPLIER_COLS)
    lead = try_cast("contract_lead_time_days", "INT")
    active = F.lower(clean("is_active"))
    df = (df
          .withColumn("supplier_id", F.upper(clean("supplier_id")))
          .withColumn("supplier_name", clean("supplier_name"))
          .withColumn("country_code", F.upper(clean("country_code")))
          .withColumn("region", F.upper(clean("region")))
          .withColumn("default_currency", std_currency("default_currency"))
          .withColumn("contract_lead_time_days", F.when(lead > 0, lead))
          .withColumn("risk_rating", F.upper(clean("risk_rating")))
          .withColumn("is_active", F.when(active.isin("true", "1", "yes", "y"), True)
                                    .when(active.isin("false", "0", "no", "n"), False))
          .withColumn("onboarded_date", try_cast("onboarded_date", "DATE")))
    df = bronze_issues(df)
    df = add_issues(
        df,
        required_issue("supplier_id"),
        required_issue("supplier_name", FLAG),
        *currency_issues("default_currency", NULLIFY),
        type_issue("contract_lead_time_days", lead, "integer", NULLIFY),
        issue(lead <= 0, "invalid_value", NULLIFY, "contract_lead_time_days",
              "contract_lead_time_days must be > 0"),
        issue(clean("is_active").isNotNull() & F.col("is_active").isNull(), "invalid_type", NULLIFY,
              "is_active", F.concat(F.lit("is_active '"), raw("is_active"), F.lit("' is not a boolean"))),
        type_issue("onboarded_date", F.col("onboarded_date"), "date (yyyy-MM-dd)", NULLIFY),
        issue(F.col("country_code").isNotNull() & ~F.col("country_code").rlike("^[A-Z]{2}$"),
              "invalid_domain", FLAG, "country_code", "country_code is not ISO 3166 alpha-2"),
        domain_issue("region", F.col("region"), REGIONS),
        domain_issue("risk_rating", F.col("risk_rating"), RISK_RATINGS),
    )
    return flag_duplicates(df, ["supplier_id"], "duplicate_supplier", "supplier_id",
                           [F.col("source_line_number")])


SILVER_SUPPLIER_COLS = SUPPLIER_COLS


# =========================================================================== #
# Products  (full snapshot -> latest file; FK to accepted suppliers)
# =========================================================================== #
PRODUCT_COLS = ["product_id", "product_name", "category", "unit_of_measure", "unit_cost",
                "cost_currency", "criticality", "primary_supplier_id", "alternate_supplier_id",
                "stocking_location_id", "on_hand_qty", "safety_stock_qty", "avg_daily_demand"]
_QTY_COLS = ["on_hand_qty", "safety_stock_qty", "avg_daily_demand"]


def check_products(bronze: DataFrame, suppliers: DataFrame) -> DataFrame:
    df = prepare(latest_snapshot(bronze), PRODUCT_COLS)
    unit_cost = try_cast("unit_cost", "DECIMAL(18,4)")
    qty = {c: try_cast(c, "DECIMAL(18,2)") for c in _QTY_COLS}
    df = (df
          .withColumn("product_id", F.upper(clean("product_id")))
          .withColumn("product_name", clean("product_name"))
          .withColumn("category", clean("category"))
          .withColumn("unit_of_measure", F.upper(clean("unit_of_measure")))
          .withColumn("unit_cost", F.when(unit_cost >= 0, unit_cost))
          .withColumn("cost_currency", std_currency("cost_currency"))
          .withColumn("criticality", F.upper(clean("criticality")))
          .withColumn("primary_supplier_id", F.upper(clean("primary_supplier_id")))
          .withColumn("alternate_supplier_id", F.upper(clean("alternate_supplier_id")))
          .withColumn("stocking_location_id", F.upper(clean("stocking_location_id"))))
    for c, typed in qty.items():
        df = df.withColumn(c, F.when(typed >= 0, typed))
    df = bronze_issues(df)
    df = add_issues(
        df,
        required_issue("product_id"),
        required_issue("product_name", FLAG),
        type_issue("unit_cost", unit_cost, "decimal", NULLIFY),
        issue(unit_cost < 0, "negative_price", NULLIFY, "unit_cost",
              F.concat(F.lit("unit_cost is negative ("), raw("unit_cost"), F.lit(")"))),
        *currency_issues("cost_currency", NULLIFY),
        *[type_issue(c, t, "decimal", NULLIFY) for c, t in qty.items()],
        *[issue(t < 0, "negative_quantity", NULLIFY, c,
                F.concat(F.lit(f"{c} is negative ("), raw(c), F.lit(")"))) for c, t in qty.items()],
        domain_issue("category", F.col("category"), PRODUCT_CATEGORIES),
        domain_issue("unit_of_measure", F.col("unit_of_measure"), UNITS_OF_MEASURE),
        domain_issue("criticality", F.col("criticality"), CRITICALITY),
    )
    df = key_exists(df, "primary_supplier_id", suppliers, "supplier_id", "_primary_ok")
    df = key_exists(df, "alternate_supplier_id", suppliers, "supplier_id", "_alternate_ok")
    df = add_issues(
        df,
        issue(F.col("primary_supplier_id").isNotNull() & ~F.col("_primary_ok"), "unknown_supplier",
              FLAG, "primary_supplier_id",
              F.concat(F.lit("primary_supplier_id "), F.col("primary_supplier_id"),
                       F.lit(" does not exist in silver_suppliers"))),
        issue(F.col("alternate_supplier_id").isNotNull() & ~F.col("_alternate_ok"), "unknown_supplier",
              NULLIFY, "alternate_supplier_id",
              F.concat(F.lit("alternate_supplier_id "), F.col("alternate_supplier_id"),
                       F.lit(" does not exist in silver_suppliers"))),
    )
    # optional alternate supplier: an unknown id is removed (NULLIFY) so Gold joins stay clean
    df = df.withColumn("alternate_supplier_id",
                       F.when(F.col("_alternate_ok"), F.col("alternate_supplier_id")))
    df = df.drop("_primary_ok", "_alternate_ok")
    return flag_duplicates(df, ["product_id"], "duplicate_product", "product_id",
                           [F.col("source_line_number")])


SILVER_PRODUCT_COLS = PRODUCT_COLS


# =========================================================================== #
# Purchase-order lines  (incremental: latest last_modified_ts wins)
# =========================================================================== #
PO_COLS = ["po_id", "po_line_number", "supplier_id", "product_id", "order_date",
           "promised_delivery_date", "ordered_qty", "unit_price", "currency",
           "delivery_location_id", "line_status", "last_modified_ts"]


def type_purchase_orders(bronze: DataFrame) -> DataFrame:
    df = prepare(bronze, PO_COLS)
    return (df
            .withColumn("po_id", F.upper(clean("po_id")))
            .withColumn("po_line_number", try_cast("po_line_number", "INT"))
            .withColumn("supplier_id", F.upper(clean("supplier_id")))
            .withColumn("product_id", F.upper(clean("product_id")))
            .withColumn("order_date", try_cast("order_date", "DATE"))
            .withColumn("promised_delivery_date", try_cast("promised_delivery_date", "DATE"))
            .withColumn("ordered_qty", try_cast("ordered_qty", "DECIMAL(18,2)"))
            .withColumn("unit_price", try_cast("unit_price", "DECIMAL(18,4)"))
            .withColumn("currency", std_currency("currency"))
            .withColumn("delivery_location_id", F.upper(clean("delivery_location_id")))
            .withColumn("line_status", F.upper(clean("line_status")))
            .withColumn("last_modified_ts", F.try_to_timestamp(clean("last_modified_ts"),
                                                               F.lit(TIMESTAMP_FORMAT))))


def check_purchase_orders(bronze: DataFrame, suppliers: DataFrame, products: DataFrame,
                          rates: DataFrame) -> DataFrame:
    df = bronze_issues(type_purchase_orders(bronze))

    # -- version resolution --------------------------------------------------
    # Keep the versions with the latest last_modified_ts. Among those, the first
    # occurrence (oldest file, lowest line) wins. Older versions are superseded
    # (normal incremental behaviour, not an issue). The same record re-sent in a
    # later file is an overlapping delivery and is dropped silently.
    key = ["po_id", "po_line_number"]
    w_ts = Window.partitionBy(*key).orderBy(F.col("last_modified_ts").desc_nulls_last())
    df = df.withColumn("_version_rank", F.dense_rank().over(w_ts)).where("_version_rank = 1")
    w = Window.partitionBy(*key).orderBy("batch_id", "source_line_number")
    wf = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = (df.withColumn("_rn", F.row_number().over(w))
            .withColumn("_first_file", F.first("source_file_name").over(wf))
            .withColumn("_first_line", F.first("source_line_number").over(wf))
            .withColumn("_first_hash", F.first("record_hash").over(wf)))
    overlap = (F.col("_rn") > 1) & (F.col("record_hash") == F.col("_first_hash")) & \
              (F.col("source_file_name") != F.col("_first_file"))
    df = df.where(~overlap)
    dup_desc = F.concat(
        F.when(F.col("record_hash") == F.col("_first_hash"), F.lit("Exact duplicate of order line ("))
         .otherwise(F.lit("Duplicate order line (")),
        F.col("po_id"), F.lit(", "), F.col("po_line_number").cast("string"),
        F.lit(") with same last_modified_ts - first occurrence on line "),
        F.col("_first_line").cast("string"))
    df = add_issues(df, issue(F.col("_rn") > 1, "duplicate_order_line", QUARANTINE,
                              "po_id,po_line_number", dup_desc))
    df = df.drop("_version_rank", "_rn", "_first_file", "_first_line", "_first_hash")

    # -- record checks ---------------------------------------------------------
    df = key_exists(df, "supplier_id", suppliers, "supplier_id", "_sup_ok")
    df = key_exists(df, "product_id", products, "product_id", "_prd_ok")
    sup_cur = suppliers.select(F.col("supplier_id").alias("_s_id"),
                               F.col("default_currency").alias("_supplier_currency"))
    df = df.join(F.broadcast(sup_cur), df["supplier_id"] == sup_cur["_s_id"], "left").drop("_s_id")
    df = with_fx(df, rates, "currency", F.col("order_date"))
    df = add_issues(
        df,
        required_issue("po_id"),
        type_issue("po_line_number", F.col("po_line_number"), "integer", QUARANTINE),
        required_issue("po_line_number"),
        type_issue("order_date", F.col("order_date"), "date (yyyy-MM-dd)", QUARANTINE),
        required_issue("order_date"),
        type_issue("promised_delivery_date", F.col("promised_delivery_date"), "date (yyyy-MM-dd)",
                   QUARANTINE),
        type_issue("ordered_qty", F.col("ordered_qty"), "decimal", QUARANTINE),
        required_issue("ordered_qty"),
        type_issue("unit_price", F.col("unit_price"), "decimal", QUARANTINE),
        type_issue("last_modified_ts", F.col("last_modified_ts"), "ISO-8601 timestamp", FLAG),
        required_issue("currency"),
        *currency_issues("currency", QUARANTINE),
        issue(F.col("currency").isNotNull() & F.col("_supplier_currency").isNotNull()
              & (F.col("currency") != F.col("_supplier_currency")),
              "currency_inconsistency", FLAG, "currency",
              F.concat(F.lit("currency '"), F.col("currency"),
                       F.lit("' differs from supplier default_currency "), F.col("_supplier_currency"))),
        issue(F.col("currency").isNotNull() & F.col("order_date").isNotNull()
              & F.col("rate_to_eur").isNull(), "missing_fx_rate", QUARANTINE, "currency",
              F.concat(F.lit("No FX rate for "), F.col("currency"), F.lit(" on "),
                       F.col("order_date").cast("string"))),
        issue(F.col("ordered_qty") < 0, "negative_quantity", QUARANTINE, "ordered_qty",
              F.concat(F.lit("ordered_qty is negative ("), raw("ordered_qty"), F.lit(")"))),
        issue(F.col("ordered_qty") == 0, "invalid_value", QUARANTINE, "ordered_qty",
              "ordered_qty must be > 0 (data dictionary)"),
        issue(F.col("unit_price") < 0, "negative_price", QUARANTINE, "unit_price",
              F.concat(F.lit("unit_price is negative ("), raw("unit_price"), F.lit(")"))),
        issue(F.col("promised_delivery_date") < F.col("order_date"), "delivery_before_order",
              QUARANTINE, "promised_delivery_date",
              F.concat(F.lit("promised_delivery_date "), F.col("promised_delivery_date").cast("string"),
                       F.lit(" is before order_date "), F.col("order_date").cast("string"))),
        issue(F.col("supplier_id").isNotNull() & ~F.col("_sup_ok"), "unknown_supplier", QUARANTINE,
              "supplier_id", F.concat(F.lit("supplier_id "), F.col("supplier_id"),
                                      F.lit(" does not exist in silver_suppliers"))),
        required_issue("supplier_id"),
        issue(F.col("product_id").isNotNull() & ~F.col("_prd_ok"), "unknown_product", QUARANTINE,
              "product_id", F.concat(F.lit("product_id "), F.col("product_id"),
                                     F.lit(" does not exist in silver_products"))),
        required_issue("product_id"),
        domain_issue("line_status", F.col("line_status"), PO_LINE_STATUSES),
    )
    return df.drop("_sup_ok", "_prd_ok", "_supplier_currency")


SILVER_PO_COLS = PO_COLS[:9] + ["rate_to_eur"] + PO_COLS[9:]


def po_line_keys(bronze_po: DataFrame) -> DataFrame:
    """All typed PO lines (any version, before quarantine): target of the data-dictionary FKs
    shipment_events.po_id -> purchase_orders.po_id and items[].po_line_number -> purchase_orders."""
    return (type_purchase_orders(bronze_po).where(F.col("po_id").isNotNull())
            .select("po_id", "po_line_number", "order_date"))


# =========================================================================== #
# Shipment events  (data dictionary: flattened in Silver to one row per event x PO line)
#   step 1 - event-level checks (one row per JSON event): dedup, lifecycle, FK po_id
#   step 2 - explode items[]: one row per event x PO line, line-level checks
# =========================================================================== #
EVENT_COLS = ["event_id", "shipment_id", "po_id", "carrier", "status", "event_ts", "location",
              "eta_ts", "items", "delay_reason_code"]


def check_shipment_events(bronze: DataFrame, po_lines: DataFrame) -> DataFrame:
    po_dates = po_lines.groupBy("po_id").agg(F.min("order_date").alias("_po_order_date"))
    df = prepare(bronze, EVENT_COLS)
    df = (df
          .withColumn("event_id", F.upper(clean("event_id")))
          .withColumn("shipment_id", F.upper(clean("shipment_id")))
          .withColumn("po_id", F.upper(clean("po_id")))
          .withColumn("carrier", clean("carrier"))
          .withColumn("status", F.upper(clean("status")))
          # ISO 8601 with offset -> UTC timestamp (assignment 6.2: standardise time zones)
          .withColumn("event_ts", F.try_to_timestamp(clean("event_ts"), F.lit(TIMESTAMP_FORMAT)))
          .withColumn("eta_ts", F.try_to_timestamp(clean("eta_ts"), F.lit(TIMESTAMP_FORMAT)))
          .withColumn("location_id", F.upper(F.trim(F.col("location__raw.location_id"))))
          .withColumn("location_name", F.trim(F.col("location__raw.name")))
          .withColumn("location_country", F.upper(F.trim(F.col("location__raw.country"))))
          .withColumn("location_type", F.upper(F.trim(F.col("location__raw.type"))))
          .withColumn("delay_reason_code", F.upper(clean("delay_reason_code"))))
    df = bronze_issues(df)
    df = key_exists(df, "po_id", po_dates, "po_id", "_po_ok")
    pd = po_dates.withColumnRenamed("po_id", "_pd_po")
    df = df.join(F.broadcast(pd), df["po_id"] == pd["_pd_po"], "left").drop("_pd_po")
    df = add_issues(
        df,
        required_issue("event_id"),
        required_issue("shipment_id"),
        required_issue("po_id"),
        required_issue("status"),
        required_issue("event_ts"),
        type_issue("event_ts", F.col("event_ts"), "ISO-8601 timestamp with offset", QUARANTINE),
        type_issue("eta_ts", F.col("eta_ts"), "ISO-8601 timestamp with offset", NULLIFY),
        domain_issue("status", F.col("status"), EVENT_STATUSES, QUARANTINE),
        domain_issue("location.type", F.col("location_type"), LOCATION_TYPES),
        domain_issue("delay_reason_code", F.col("delay_reason_code"), DELAY_REASON_CODES),
        issue(F.size(F.coalesce(F.col("items__raw"), F.array())) == 0, "missing_required", QUARANTINE,
              "items", "event has no items"),
        issue(F.col("po_id").isNotNull() & ~F.col("_po_ok"), "unknown_purchase_order", QUARANTINE, "po_id",
              F.concat(F.lit("po_id "), F.col("po_id"), F.lit(" does not exist in purchase_orders"))),
        issue((F.col("status") == "DELIVERED") & (F.col("event_ts") < F.col("_po_order_date")),
              "delivery_before_order", QUARANTINE, "event_ts",
              F.concat(F.lit("DELIVERED event_ts is before PO order_date "),
                       F.col("_po_order_date").cast("string"))),
    )
    df = df.drop("_po_ok", "_po_order_date")

    # -- duplicates: same event_id. Identical record re-sent in another file = overlap (dropped)
    w = Window.partitionBy("event_id").orderBy("batch_id", "source_line_number")
    wf = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = (df.withColumn("_rn", F.row_number().over(w))
            .withColumn("_first_file", F.first("source_file_name").over(wf))
            .withColumn("_first_hash", F.first("record_hash").over(wf)))
    df = df.where(~((F.col("_rn") > 1) & (F.col("record_hash") == F.col("_first_hash"))
                    & (F.col("source_file_name") != F.col("_first_file"))))
    df = add_issues(df, issue(
        F.col("_rn") > 1, "duplicate_event", QUARANTINE, "event_id",
        F.concat(F.when(F.col("record_hash") == F.col("_first_hash"), F.lit("Exact duplicate of event_id "))
                  .otherwise(F.lit("Conflicting duplicate of event_id ")), F.col("event_id"))))
    df = df.drop("_rn", "_first_file", "_first_hash")

    # -- lifecycle (invalid status sequence), evaluated on events not already blocked
    cand = ~is_blocked()
    ship = Window.partitionBy("shipment_id")
    ts, st = F.col("event_ts"), F.col("status")
    df = df.withColumn("_first_pickup", F.min(F.when(cand & (st == "PICKED_UP"), ts)).over(ship))
    early_delivery = (st == "DELIVERED") & F.col("_first_pickup").isNotNull() & (ts < F.col("_first_pickup"))
    df = add_issues(df, issue(cand & early_delivery, "invalid_status_sequence", QUARANTINE, "status",
                              "DELIVERED event occurs before PICKED_UP for the same shipment"))
    cand = ~is_blocked()
    df = df.withColumn("_delivered_ts", F.min(F.when(cand & (st == "DELIVERED"), ts)).over(ship))
    df = add_issues(df, issue(cand & (ts > F.col("_delivered_ts")), "invalid_status_sequence",
                              QUARANTINE, "status",
                              F.concat(st, F.lit(" event occurs after DELIVERED for the same shipment"))))
    return df.drop("_first_pickup", "_delivered_ts")


def check_shipment_event_lines(events_checked: DataFrame, products: DataFrame, po_lines: DataFrame) -> DataFrame:
    """One row per event x PO line (items[] exploded); the event's block status is carried along."""
    df = (events_checked
          .withColumn("_event_blocked", is_blocked())
          .select("event_id", "shipment_id", "po_id", "carrier", "status", "event_ts", "location_id",
                  "location_name", "location_country", "location_type", "eta_ts", "delay_reason_code",
                  "_event_blocked", "raw_payload", *LINEAGE, F.explode("items__raw").alias("_item"))
          .withColumn("po_line_number__raw", F.col("_item.po_line_number"))
          .withColumn("product_id__raw", F.col("_item.product_id"))
          .withColumn("quantity__raw", F.col("_item.quantity"))
          .drop("_item")
          .withColumn("po_line_number", try_cast("po_line_number", "INT"))
          .withColumn("product_id", F.upper(clean("product_id")))
          .withColumn("quantity", try_cast("quantity", "DECIMAL(18,2)")))
    df = key_exists(df, "product_id", products, "product_id", "_prd_ok")
    lines = po_lines.select(F.concat_ws("#", "po_id", "po_line_number").alias("_line_key")).distinct()
    df = (df.withColumn("_line_key", F.concat_ws("#", "po_id", "po_line_number"))
            .join(F.broadcast(lines.withColumn("_line_ok", F.lit(True))), "_line_key", "left")
            .withColumn("_line_ok", F.coalesce("_line_ok", F.lit(False))))
    df = add_issues(
        df,
        required_issue("po_line_number"),
        required_issue("product_id"),
        required_issue("quantity"),
        type_issue("po_line_number", F.col("po_line_number"), "integer", QUARANTINE),
        type_issue("quantity", F.col("quantity"), "decimal", QUARANTINE),
        issue(F.col("quantity") < 0, "negative_quantity", QUARANTINE, "items.quantity",
              F.concat(F.lit("items.quantity is negative ("), raw("quantity"), F.lit(") for line "),
                       F.col("po_line_number").cast("string"))),
        issue(F.col("product_id").isNotNull() & ~F.col("_prd_ok"), "unknown_product", QUARANTINE,
              "items.product_id",
              F.concat(F.lit("items.product_id "), F.col("product_id"), F.lit(" does not exist in silver_products"))),
        issue(F.col("po_line_number").isNotNull() & ~F.col("_line_ok"), "unknown_order_line", QUARANTINE,
              "items.po_line_number",
              F.concat(F.lit("PO line "), F.col("_line_key"), F.lit(" does not exist in purchase_orders"))),
    )
    return df.drop("_prd_ok", "_line_ok", "_line_key")


# Data-dictionary fields of shipment_events, flattened (location.* -> location_*, items[].* -> columns)
SILVER_EVENT_COLS = ["event_id", "shipment_id", "po_id", "carrier", "status", "event_ts", "location_id",
                     "location_name", "location_country", "location_type", "eta_ts", "po_line_number",
                     "product_id", "quantity", "delay_reason_code"]


# =========================================================================== #
# Outputs
# =========================================================================== #
def accepted(checked: DataFrame, columns: list[str], extra_filter: Column | None = None) -> DataFrame:
    df = checked.where(~is_blocked())
    if extra_filter is not None:
        df = df.where(extra_filter)
    return df.select(*columns, *LINEAGE)


def quarantined(checked: DataFrame, entity: str, record_key: str) -> DataFrame:
    """Assignment 6.2: invalid records with reason, rule and processing timestamp (one row per blocking rule)."""
    return (checked.where(is_blocked())
            .select(F.lit(entity).alias("entity"), F.expr(record_key).cast("string").alias("record_key"),
                    F.explode(F.filter("dq_issues", lambda i: i["action"] == QUARANTINE)).alias("i"),
                    "raw_payload", *LINEAGE)
            .select("entity", "record_key", F.col("i.rule").alias("rule"), F.col("i.description").alias("reason"),
                    "raw_payload", *LINEAGE, F.current_timestamp().alias("processing_ts")))


def issues(checked: DataFrame, entity: str, record_key: str) -> DataFrame:
    """Every rule hit, whatever the disposition (quality results are measurable - 6.2 acceptance)."""
    return (checked.select(F.lit(entity).alias("entity"), F.expr(record_key).cast("string").alias("record_key"),
                           *LINEAGE, F.explode("dq_issues").alias("i"))
            .select("entity", "record_key", "i.rule", "i.action", "i.column_name",
                    F.col("i.description").alias("reason"), *LINEAGE,
                    F.current_timestamp().alias("processing_ts")))


# (entity = data-dictionary dataset name, record key as SQL expression)
CHECKED_ENTITIES = [
    ("suppliers", "supplier_id"),
    ("products", "product_id"),
    ("purchase_orders", "concat_ws('#', po_id, po_line_number)"),
    ("shipment_events", "event_id"),                                       # event-level rules
    ("shipment_events", "concat_ws('#', event_id, po_line_number)"),       # event x PO line rules
]
