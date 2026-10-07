# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver verification (assignment 6.2)
# MAGIC Acceptance: *conformed records are analytically consistent, quality results are measurable, and
# MAGIC rejected records remain visible and actionable.* Fails the job on a broken invariant.

# COMMAND ----------

dbutils.widgets.text("catalog", "data2026acc123456")
dbutils.widgets.text("schema", "team4")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
S = f"{catalog}.{schema}"
from pyspark.sql import functions as F


def scalar(sql):
    return spark.sql(sql).first()[0]

# COMMAND ----------

# MAGIC %md ### 1. Quality results by rule and disposition

# COMMAND ----------

display(spark.sql(f"""SELECT entity, rule, action, COUNT(*) AS hits FROM {S}.ops_dq_issues
                      GROUP BY ALL ORDER BY entity, rule"""))

# COMMAND ----------

# MAGIC %md ### 2. Injected defects detected (answer keys in landing/_dq_expected)

# COMMAND ----------

keys = None
for src in ["suppliers", "products", "purchase_orders", "shipment_events"]:
    k = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
         .csv(f"/Volumes/{catalog}/{schema}/landing/_dq_expected/{src}_errors.csv")
         .select(F.lit(src).alias("entity"), F.col("source_line_number").cast("int").alias("source_line_number"),
                 F.col("dq_rule").alias("rule")))
    keys = k if keys is None else keys.unionByName(k)
found = (spark.table(f"{S}.ops_dq_issues").select("entity", "source_line_number", "rule").distinct()
         .withColumn("detected", F.lit(True)))
joined = keys.join(found, ["entity", "source_line_number", "rule"], "left")
display(joined.groupBy("entity", "rule").agg(F.count(F.lit(1)).alias("expected"),
                                             F.count("detected").alias("detected")).orderBy("entity", "rule"))
missed = joined.where(F.col("detected").isNull()).count()          # expected 0 (435 / 435)

# COMMAND ----------

# MAGIC %md ### 3. Quarantine: rule, reason, raw record, processing timestamp

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {S}.silver_quarantine ORDER BY entity, source_line_number LIMIT 50"))

# COMMAND ----------

# MAGIC %md ### 4. Traceability: Silver row -> Bronze raw line

# COMMAND ----------

display(spark.sql(f"""
  SELECT s.po_id, s.po_line_number, s.ordered_qty, s.rate_to_eur, s.batch_id, s.source_line_number, b.raw_payload
  FROM {S}.silver_purchase_orders s JOIN {S}.bronze_purchase_orders b USING (batch_id, source_line_number)
  WHERE s.po_id = 'PO-2025-00196'"""))

# COMMAND ----------

# MAGIC %md ### 5. Invariants

# COMMAND ----------

checks = {
    "suppliers unique": scalar(f"SELECT COUNT(*) - COUNT(DISTINCT supplier_id) FROM {S}.silver_suppliers") == 0,
    "products unique": scalar(f"SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM {S}.silver_products") == 0,
    "PO lines unique": scalar(f"""SELECT COUNT(*) - COUNT(DISTINCT po_id, po_line_number)
                                  FROM {S}.silver_purchase_orders""") == 0,
    "event x line unique": scalar(f"""SELECT COUNT(*) - COUNT(DISTINCT event_id, po_line_number)
                                      FROM {S}.silver_shipment_events""") == 0,
    "PO -> supplier FK": scalar(f"""SELECT COUNT(*) FROM {S}.silver_purchase_orders
                                    LEFT ANTI JOIN {S}.silver_suppliers USING (supplier_id)""") == 0,
    "PO -> product FK": scalar(f"""SELECT COUNT(*) FROM {S}.silver_purchase_orders
                                   LEFT ANTI JOIN {S}.silver_products USING (product_id)""") == 0,
    "event -> product FK": scalar(f"""SELECT COUNT(*) FROM {S}.silver_shipment_events
                                      LEFT ANTI JOIN {S}.silver_products USING (product_id)""") == 0,
    "all injected defects detected": missed == 0,
}
display(spark.createDataFrame(list(checks.items()), "check string, passed boolean"))
assert all(checks.values()), [k for k, v in checks.items() if not v]
