# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold verification (assignment 6.3)
# MAGIC Star-schema integrity, Silver -> Gold reconciliation, and the 8 brief KPIs computed exactly like the
# MAGIC DAX measures (src/gold/config.py) for validating the report. Fails the job on a broken invariant.

# COMMAND ----------

dbutils.widgets.text("catalog", "data2026acc123456")
dbutils.widgets.text("schema", "team4")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
S = f"{catalog}.{schema}"


def scalar(sql):
    return spark.sql(sql).first()[0]

# COMMAND ----------

# MAGIC %md ### 1. Dimensions: unique keys and Unknown member (-1)

# COMMAND ----------

DIMS = {"gold_dim_date": "date_key", "gold_dim_supplier": "supplier_key", "gold_dim_product": "product_key",
        "gold_dim_location": "location_key", "gold_dim_shipment_status": "shipment_status_key",
        "gold_dim_risk_level": "risk_level_key"}
dims = {t: (scalar(f"SELECT COUNT(*) - COUNT(DISTINCT {k}) FROM {S}.{t}") == 0
            and scalar(f"SELECT COUNT(*) FROM {S}.{t} WHERE {k} = -1") == 1) for t, k in DIMS.items()}

# COMMAND ----------

# MAGIC %md ### 2. Referential integrity (orphan fact keys)

# COMMAND ----------

FKS = [("gold_fact_purchase_order_line", c, d) for c, d in [
          ("supplier_key", "gold_dim_supplier"), ("product_key", "gold_dim_product"),
          ("location_key", "gold_dim_location"), ("risk_level_key", "gold_dim_risk_level"),
          ("order_date_key", "gold_dim_date"), ("promised_delivery_date_key", "gold_dim_date"),
          ("actual_delivery_date_key", "gold_dim_date"), ("latest_eta_date_key", "gold_dim_date")]] + \
      [("gold_fact_shipment_event", c, d) for c, d in [
          ("shipment_status_key", "gold_dim_shipment_status"), ("supplier_key", "gold_dim_supplier"),
          ("location_key", "gold_dim_location"), ("event_date_key", "gold_dim_date")]] + \
      [("gold_fact_inventory_exposure", c, d) for c, d in [
          ("product_key", "gold_dim_product"), ("supplier_key", "gold_dim_supplier"),
          ("location_key", "gold_dim_location"), ("risk_level_key", "gold_dim_risk_level"),
          ("snapshot_date_key", "gold_dim_date")]]
orphans = {f"{f}.{c}": scalar(f"SELECT COUNT(*) FROM {S}.{f} x LEFT ANTI JOIN {S}.{d} y ON x.{c} = y.{DIMS[d]}")
           for f, c, d in FKS}

# COMMAND ----------

# MAGIC %md ### 3. Silver -> Gold reconciliation (row counts and critical totals)

# COMMAND ----------

ov = "SUM(ordered_qty * unit_price * rate_to_eur)"
recon = {
    "PO lines (non-cancelled)": (scalar(f"SELECT COUNT(*) FROM {S}.gold_fact_purchase_order_line"),
                                 scalar(f"SELECT COUNT(*) FROM {S}.silver_purchase_orders "
                                        f"WHERE line_status <> 'CANCELLED'")),
    "Order Value": (scalar(f"SELECT ROUND({ov}, 2) FROM {S}.gold_fact_purchase_order_line"),
                    scalar(f"SELECT ROUND({ov}, 2) FROM {S}.silver_purchase_orders WHERE line_status <> 'CANCELLED'")),
    "Shipment events": (scalar(f"SELECT COUNT(*) FROM {S}.gold_fact_shipment_event"),
                        scalar(f"SELECT COUNT(DISTINCT event_id) FROM {S}.silver_shipment_events")),
    "Products": (scalar(f"SELECT COUNT(*) FROM {S}.gold_fact_inventory_exposure"),
                 scalar(f"SELECT COUNT(*) FROM {S}.silver_products")),
}
display(spark.createDataFrame([(k, str(g), str(s), g == s) for k, (g, s) in recon.items()],
                              "check string, gold string, silver string, match boolean"))

# COMMAND ----------

# MAGIC %md ### 4. The 8 brief KPIs (same logic as the DAX measures)

# COMMAND ----------

display(spark.sql(f"""
  WITH l AS (SELECT * FROM {S}.gold_fact_purchase_order_line),
       e AS (SELECT * FROM {S}.gold_fact_inventory_exposure),
       p AS (SELECT * FROM {S}.gold_dim_product WHERE criticality = 'CRITICAL'),
       s AS (SELECT MAX(snapshot_date_key) AS k FROM e)
  SELECT (SELECT ROUND({ov}, 2) FROM l)                                                    AS order_value_eur,
         (SELECT ROUND(COUNT_IF(is_on_time) / COUNT_IF(is_delivered), 4) FROM l)            AS on_time_delivery_rate,
         (SELECT ROUND(AVG(delay_days), 2) FROM l WHERE is_delivered AND delay_days > 0)    AS average_delay_days,
         (SELECT ROUND(SUM(LEAST(received_qty, ordered_qty)) / SUM(ordered_qty), 4)
            FROM l, s WHERE promised_delivery_date_key <= s.k)                              AS supplier_fill_rate,
         (SELECT ROUND(SUM(delayed_inbound_qty * unit_cost * rate_to_eur), 2) FROM e
            WHERE days_of_cover < days_until_arrival)                                       AS exposed_value_eur,
         (SELECT SUM(delayed_qty) FROM l)                                                   AS delayed_quantity,
         (SELECT COUNT(DISTINCT e.product_key) FROM e JOIN p USING (product_key)
            WHERE delayed_inbound_qty > 0)                                                  AS critical_item_count,
         (SELECT ROUND(COUNT_IF(has_active_alternate) / COUNT(*), 4) FROM p)                AS alternate_supplier_coverage
"""))
# Generated data, snapshot 2026-10-05 (placeholder FX): 190,388,429.60 | 0.6446 | 6.74 | 0.8426 |
# 2,574,955.37 | 6,929,712.71 | 22 | 0.5862

# COMMAND ----------

problems = ([t for t, ok in dims.items() if not ok] + [k for k, n in orphans.items() if n]
            + [k for k, (g, s) in recon.items() if g != s])
assert not problems, f"Gold checks failed: {problems}"
print("All Gold checks passed")
