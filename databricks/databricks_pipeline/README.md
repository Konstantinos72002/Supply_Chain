# Team 4 – Supply Chain Disruption Analytics (Databricks medallion)

Catalog `data2026acc123456`, schema `team4`. One Lakeflow ETL pipeline, raw files → Bronze → Silver → Gold.
Table and column names follow `data_dictionary.md` and the Team 4 assignment table, prefixed with the layer
(`bronze_`, `silver_`, `gold_`, `ops_`) because all layers share one schema.

```
pipelines/medallion/           ONE Lakeflow ETL pipeline, 3 source files in one dependency graph
  01_bronze.py                 landing files -> bronze_* (Auto Loader), ops_bronze_ingestion_log
  02_silver.py                 bronze_* -> silver_*, silver_fx_rates, silver_quarantine, ops_dq_issues
  03_gold.py                   silver_* -> gold_fact_* (3) + gold_dim_* (6)
src/bronze/                    source contracts (config.py) + transformations
src/silver/                    reference data, fx_rates seed, DQ policy (reference.py) + transformations
src/gold/                      KPI / DAX definitions (config.py), transformations, column docs (docs.py)
notebooks/00_setup_unity_catalog.sql   landing Volume + folders (run once)
notebooks/02..04_*_checks.py   Bronze / Silver / Gold verification (fail on a broken invariant)
resources/pipelines.yml        Asset Bundle: the pipeline + job (pipeline -> 3 check notebooks)
tests/                         pytest: unit tests, brief-KPI reference, end-to-end run of the pipeline
docs/                          data_model.md (generated), design_notes.md (refresh, SCD, mapping, assumptions)
```

## Run
1. `00_setup_unity_catalog.sql` once. Upload files to `/Volumes/data2026acc123456/team4/landing/<source>/`,
   the `*_errors.csv` answer keys to `landing/_dq_expected/`.
2. Pipeline `team4-supply-chain-medallion` with the 3 source files and configuration
   `pipeline.src_path`, `bronze.landing_root`, `gold.as_of_date` (optional = brief snapshot_date),
   `spark.sql.session.timeZone=UTC`. **Full refresh all** after schema changes.
3. Notebooks `02_bronze_checks`, `03_silver_checks`, `04_gold_checks`.

## Tables
| Layer | Tables |
|---|---|
| Bronze | `bronze_suppliers`, `bronze_products`, `bronze_purchase_orders`, `bronze_shipment_events`, `ops_bronze_ingestion_log` |
| Silver | `silver_suppliers`, `silver_products`, `silver_purchase_orders`, `silver_shipment_events` (event × PO line), `silver_fx_rates`, `silver_quarantine`, `ops_dq_issues` |
| Gold | `gold_fact_purchase_order_line`, `gold_fact_shipment_event`, `gold_fact_inventory_exposure`, `gold_dim_date`, `gold_dim_supplier`, `gold_dim_product`, `gold_dim_location`, `gold_dim_shipment_status`, `gold_dim_risk_level` |

The 8 brief KPIs are DAX measures of the semantic model; definitions in `src/gold/config.py` and
`docs/data_model.md`. **FX rates in `src/silver/reference.py` are placeholders.**

## Tests
```
pip install -r requirements-dev.txt
BRONZE_TEST_DATA=/path/to/generated/files pytest -q tests/          # ~10 minutes
BRONZE_TEST_DATA=/path/to/generated/files python tests/render_gold_docs.py   # regenerate docs/data_model.md
```
