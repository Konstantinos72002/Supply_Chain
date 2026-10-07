# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze verification (assignment 6.1)
# MAGIC Acceptance: *trace any downstream record to its source file and ingestion batch, and rerun a failed
# MAGIC batch without uncontrolled duplicates.*

# COMMAND ----------

dbutils.widgets.text("catalog", "data2026acc123456")
dbutils.widgets.text("schema", "team4")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
S = f"{catalog}.{schema}"
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ### 1. Ingestion log: input, written and rejected counts per batch

# COMMAND ----------

display(spark.table(f"{S}.ops_bronze_ingestion_log").orderBy("target_table", "batch_id"))
# First load: suppliers 51, products 302, purchase_orders 4,020, shipment_events 9,431 (13,804); rejected 0

# COMMAND ----------

# MAGIC %md ### 2. Source lines vs Bronze rows (independent check against the landing files)

# COMMAND ----------

root = f"/Volumes/{catalog}/{schema}/landing"
rows = []
for table, folder, header in [("bronze_suppliers", "suppliers", 1), ("bronze_products", "products", 1),
                              ("bronze_purchase_orders", "purchase_orders", 1),
                              ("bronze_shipment_events", "shipment_events", 0)]:
    lines = (spark.read.text(f"{root}/{folder}")
             .where(~F.col("_metadata.file_name").endswith("_errors.csv")).where(F.trim("value") != "")
             .groupBy(F.col("_metadata.file_path").alias("f")).count().collect())
    for r in lines:
        b = spark.table(f"{S}.{table}").where(F.col("source_file_name") == r.f).count()
        rows.append((table, r.f, r["count"] - header, b, r["count"] - header == b))
display(spark.createDataFrame(rows, "table string, file string, source_rows long, bronze_rows long, match boolean"))

# COMMAND ----------

# MAGIC %md ### 3. Lineage: a record back to its file, line and batch

# COMMAND ----------

display(spark.sql(f"""SELECT po_id, po_line_number, source_file_name, source_line_number, batch_id, ingestion_ts,
                             load_status FROM {S}.bronze_purchase_orders WHERE po_id = 'PO-2025-00003'"""))

# COMMAND ----------

# MAGIC %md ### 4. No uncontrolled duplicates after reruns (one row per file line)

# COMMAND ----------

display(spark.sql(f"""SELECT batch_id, source_line_number, COUNT(*) AS copies FROM {S}.bronze_purchase_orders
                      GROUP BY ALL HAVING COUNT(*) > 1"""))     # must be empty

# COMMAND ----------

# MAGIC %md ### 5. Duration and failures (pipeline event log)

# COMMAND ----------

display(spark.sql(f"""
  SELECT timestamp, origin.flow_name, details:flow_progress.status AS status,
         details:flow_progress.metrics.num_output_rows::bigint AS output_rows, error
  FROM {S}.ops_pipeline_event_log
  WHERE event_type = 'flow_progress' AND origin.flow_name LIKE 'bronze_%' ORDER BY timestamp DESC"""))
