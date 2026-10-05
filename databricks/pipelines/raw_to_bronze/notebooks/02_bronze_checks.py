# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze verification – evidence for the acceptance condition
# MAGIC *"The team can trace any downstream record to its source file and ingestion batch and can
# MAGIC rerun a failed batch without creating uncontrolled duplicates."*
# MAGIC
# MAGIC Run after a pipeline update. Uses the Bronze tables, `ops_bronze_file_summary` and the
# MAGIC pipeline event log published as `ops_pipeline_event_log`.

# COMMAND ----------

dbutils.widgets.text("catalog", "data2026acc123456")
dbutils.widgets.text("schema", "team4")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
S = f"{catalog}.{schema}"
EVENT_LOG = f"{S}.ops_pipeline_event_log"

# COMMAND ----------

# MAGIC %md ### 1. Reconciliation per file version (rows written by load status)

# COMMAND ----------

display(spark.sql(f"""
  SELECT source_name, source_file_name, batch_id, rows_written, rows_loaded,
         rows_schema_drift, rows_malformed, first_ingestion_ts
  FROM {S}.ops_bronze_file_summary ORDER BY source_name, source_file_mod_ts"""))
# Expected first load: 51 suppliers, 302 products, 4,020 PO lines, 9,431 events (13,804 rows)

# COMMAND ----------

# MAGIC %md ### 2. Source line count vs Bronze row count (independent check against the raw files)

# COMMAND ----------

from pyspark.sql import functions as F

root = f"/Volumes/{catalog}/{schema}/landing"
checks = [("bronze_suppliers_raw", "suppliers", True), ("bronze_products_raw", "products", True),
          ("bronze_purchase_orders_raw", "purchase_orders", True),
          ("bronze_shipment_events_raw", "shipment_events", False)]
rows = []
for table, folder, has_header in checks:
    lines = (spark.read.text(f"{root}/{folder}")
             .where(~F.col("_metadata.file_name").endswith("_errors.csv"))
             .where(F.trim("value") != "")
             .groupBy(F.col("_metadata.file_path").alias("source_file_name")).count())
    for r in lines.collect():
        source_rows = r["count"] - (1 if has_header else 0)
        bronze_rows = spark.table(f"{S}.{table}").where(
            F.col("source_file_name") == r.source_file_name).count()
        rows.append((table, r.source_file_name, source_rows, bronze_rows, source_rows == bronze_rows))
display(spark.createDataFrame(rows, "table string, file string, source_rows long, bronze_rows long, match boolean"))

# COMMAND ----------

# MAGIC %md ### 3. Lineage: trace one record back to its file and batch

# COMMAND ----------

display(spark.sql(f"""
  SELECT po_id, po_line_number, source_file_name, source_file_mod_ts, batch_id, ingestion_ts, load_status
  FROM {S}.bronze_purchase_orders_raw WHERE po_id = 'PO-2025-00003'"""))

# COMMAND ----------

# MAGIC %md ### 4. No uncontrolled duplicates (rerun / full refresh must not copy rows)

# COMMAND ----------

display(spark.sql(f"""
  SELECT batch_id, record_hash, COUNT(*) AS copies
  FROM {S}.bronze_purchase_orders_raw GROUP BY ALL HAVING COUNT(*) > 1"""))
# Rows here can only be genuine duplicate lines inside a source file (kept on purpose and
# deduplicated in Silver), never copies created by re-running the pipeline.

# COMMAND ----------

# MAGIC %md ### 5. Defects are preserved in Bronze (not fixed)

# COMMAND ----------

display(spark.sql(f"""SELECT supplier_id, default_currency, batch_id
                      FROM {S}.bronze_suppliers_raw WHERE supplier_id IN ('SUP-0007','SUP-0050')"""))

# COMMAND ----------

# MAGIC %md ### 6. Pipeline monitoring from the event log: rows per flow and update

# COMMAND ----------

display(spark.sql(f"""
  SELECT timestamp, origin.update_id, origin.flow_name,
         details:flow_progress.status AS status,
         details:flow_progress.metrics.num_output_rows::bigint AS output_rows
  FROM {EVENT_LOG}
  WHERE event_type = 'flow_progress' AND details:flow_progress.metrics.num_output_rows IS NOT NULL
  ORDER BY timestamp DESC"""))

# COMMAND ----------

# MAGIC %md ### 7. Expectation results (schema drift, missing keys, malformed JSON)

# COMMAND ----------

display(spark.sql(f"""
  SELECT timestamp, origin.flow_name, e.name AS expectation, e.passed_records, e.failed_records
  FROM {EVENT_LOG}
  LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) x AS e
  WHERE event_type = 'flow_progress'
  ORDER BY timestamp DESC"""))

# COMMAND ----------

# MAGIC %md ### 8. Failures and errors

# COMMAND ----------

display(spark.sql(f"""
  SELECT timestamp, level, origin.flow_name, message, error
  FROM {EVENT_LOG} WHERE level IN ('ERROR', 'WARN') ORDER BY timestamp DESC"""))

# COMMAND ----------

# MAGIC %md ### 9. Delta history (audit trail of every write)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {S}.bronze_purchase_orders_raw"))
