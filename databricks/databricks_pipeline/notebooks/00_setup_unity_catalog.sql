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
-- MAGIC | `team4.silver_*` | Validated, conformed tables + `silver_quarantine` |
-- MAGIC | `team4.gold_*` | Star schema for Fabric / Power BI |
-- MAGIC | `team4.ops_*` | Pipeline event log, file summary, DQ results |
-- MAGIC
-- MAGIC Tables are created by the single **ETL pipeline** `team4-supply-chain-medallion`
-- MAGIC (source files `pipelines/medallion/01_bronze.py`, `02_silver.py`, `03_gold.py`), not here.
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
-- MAGIC ### Migration note
-- MAGIC Tables are owned by the ETL pipeline `team4-supply-chain-medallion`. If tables from earlier versions
-- MAGIC exist (e.g. `bronze_*_raw`, `silver_purchase_order_lines`, `gold_fact_inventory_risk`), delete the
-- MAGIC pipeline(s) that created them before creating/updating the new one: deleting a pipeline drops its tables.
-- MAGIC Files in the landing Volume are never affected; the pipeline rebuilds everything from them.

-- COMMAND ----------

SHOW VOLUMES;
SHOW TABLES;

-- COMMAND ----------

-- Optional, if your schema-level permissions allow it (ask the workspace admin otherwise).
-- Least-privilege examples at object level inside team4:
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME data2026acc123456.team4.landing TO `team4-data-engineers`;
-- GRANT SELECT ON TABLE data2026acc123456.team4.ops_bronze_ingestion_log TO `team4-analysts`;
