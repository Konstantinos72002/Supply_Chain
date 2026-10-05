-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Unity Catalog setup – Team 4 Supply Chain
-- MAGIC We work inside the organisation catalog **`data2026acc123456`** and only have access to
-- MAGIC schema **`team4`**, so we do not create catalogs or schemas. The medallion layers are
-- MAGIC separated by **table-name prefix** inside `team4`:
-- MAGIC
-- MAGIC | Object | Purpose |
-- MAGIC |---|---|
-- MAGIC | Volume `data2026acc123456.team4.landing` | Raw CSV/JSON files exactly as delivered (one folder per source) |
-- MAGIC | `team4.bronze_*` | Raw Delta tables + audit columns |
-- MAGIC | `team4.silver_*` | Validated, conformed tables (next step) |
-- MAGIC | `team4.gold_*` | Star schema for Fabric / Power BI |
-- MAGIC | `team4.ops_*` | Pipeline event log, file summary, DQ results |
-- MAGIC
-- MAGIC Tables are created by the **ETL pipeline** (`pipelines/bronze_pipeline.py`), not here.
-- MAGIC
-- MAGIC Required privileges on `team4`: `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE VOLUME`
-- MAGIC (plus `USE CATALOG` on `data2026acc123456`).

-- COMMAND ----------

USE CATALOG data2026acc123456;
USE SCHEMA team4;

-- Sanity check: confirms we are in the right place and lists what already exists
SELECT current_catalog() AS catalog, current_schema() AS schema;

-- COMMAND ----------

-- Landing zone for raw files
CREATE VOLUME IF NOT EXISTS landing
  COMMENT 'Landing zone: raw source files as delivered, one sub-folder per source';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # One folder per source. _dq_expected holds the *_errors.csv answer keys (used to test Silver, never ingested).
-- MAGIC root = "/Volumes/data2026acc123456/team4/landing"
-- MAGIC for folder in ["suppliers", "products", "purchase_orders", "shipment_events", "_dq_expected"]:
-- MAGIC     dbutils.fs.mkdirs(f"{root}/{folder}")
-- MAGIC display(dbutils.fs.ls(root))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Clean-up from the notebook-based version (run once)
-- MAGIC The ETL pipeline must **own** its tables. Tables with the same names created earlier by the
-- MAGIC notebook would make the pipeline fail with "table already exists". Drop them once.
-- MAGIC Your raw files in the Volume are NOT touched.

-- COMMAND ----------

DROP TABLE IF EXISTS bronze_suppliers_raw;
DROP TABLE IF EXISTS bronze_products_raw;
DROP TABLE IF EXISTS bronze_purchase_orders_raw;
DROP TABLE IF EXISTS bronze_shipment_events_raw;
DROP TABLE IF EXISTS ops_ingestion_log;   -- replaced by the pipeline event log

-- COMMAND ----------

SHOW VOLUMES;
SHOW TABLES;

-- COMMAND ----------

-- Optional, if your schema-level permissions allow it (ask the workspace admin otherwise).
-- Least-privilege examples at object level inside team4:
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data2026acc123456.team4.landing TO `team4-data-engineers`;
-- GRANT SELECT ON TABLE data2026acc123456.team4.ops_bronze_file_summary TO `team4-analysts`;
