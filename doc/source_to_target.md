# Source-to-Target Mapping – Supply Chain Disruption Analytics

Living document. It maps every source field to its target in each medallion layer.

| Section | Scope | Status |
|---|---|---|
| 1. Raw → Bronze | Landing files → `bronze_*_raw` Delta tables | ✅ v1 (#95) |
| 2. Bronze → Silver | `bronze_*_raw` → `silver_*` + quarantine | ⏳ T31 |
| 3. Silver → Gold | `silver_*` → star schema (`gold_*`) | ⏳ Gold phase |

Catalog / schema: `data2026acc123456.team4`. Layers are separated by table-name prefix (`bronze_`, `silver_`, `gold_`, `ops_`).
Source of truth for the Bronze contract: `databricks/pipelines/raw_to_bronze/src/bronze/config.py`.

---

## 1. Raw → Bronze

### 1.1 Principles

- **No business transformation.** Every value is ingested as `STRING`, so that source defects (`usd`, `-278.00`, `2025-13-40`) reach Silver unchanged and can be quarantined there.
- **Column names are unchanged.** Bronze column = raw field name, in contract order.
- **Append-only.** Each file version is ingested once by Auto Loader (checkpoint), so reruns do not create copies.
- **Every row is traceable** to its file and batch through the audit columns (§1.4).
- **Defects are kept, not fixed.** Duplicates, unknown keys, bad currencies and invalid dates are resolved in Silver.

### 1.2 Source → table mapping

| Raw file (landing folder) | Format | Bronze table | source_system | Load type | Business key | Raw rows (batch 1) |
|---|---|---|---|---|---|---|
| `landing/suppliers/suppliers*.csv` | CSV, header | `bronze_suppliers_raw` | `ERP_MASTER_DATA` | Full snapshot per delivery, appended as a new batch | `supplier_id` | 51 |
| `landing/products/products*.csv` | CSV, header | `bronze_products_raw` | `ERP_MASTER_DATA` | Full snapshot per delivery, appended as a new batch | `product_id` | 302 |
| `landing/purchase_orders/purchase_orders*.csv` | CSV, header | `bronze_purchase_orders_raw` | `ERP_PROCUREMENT` | Incremental (latest `last_modified_ts` wins, resolved in Silver) | `po_id`, `po_line_number` | 4,020 |
| `landing/shipment_events/shipment_events*.json` | NDJSON (one object per line) | `bronze_shipment_events_raw` | `CARRIER_TMS` | Incremental | `event_id` | 9,431 |

Total batch 1: **13,804 rows** (13,451 transactional/event rows).

`fx_rates` is **not** a raw source. It is a configuration seed table created in Silver, so it has no Bronze mapping.

### 1.3 Column mapping

All CSV sources follow the same rule: **raw column → Bronze column with the same name, type `STRING`, no transformation.** The target types in the last column are applied in Silver and are listed here only for reference (see `doc/data_dictionary.md`).

#### suppliers.csv → bronze_suppliers_raw

| Raw field | Bronze column | Bronze type | Target type in Silver |
|---|---|---|---|
| supplier_id | supplier_id | STRING | string (PK) |
| supplier_name | supplier_name | STRING | string |
| country_code | country_code | STRING | string |
| region | region | STRING | string |
| default_currency | default_currency | STRING | string |
| contract_lead_time_days | contract_lead_time_days | STRING | integer |
| risk_rating | risk_rating | STRING | string |
| is_active | is_active | STRING | boolean |
| onboarded_date | onboarded_date | STRING | date |

#### products.csv → bronze_products_raw

| Raw field | Bronze column | Bronze type | Target type in Silver |
|---|---|---|---|
| product_id | product_id | STRING | string (PK) |
| product_name | product_name | STRING | string |
| category | category | STRING | string |
| unit_of_measure | unit_of_measure | STRING | string |
| unit_cost | unit_cost | STRING | decimal(18,4) |
| cost_currency | cost_currency | STRING | string |
| criticality | criticality | STRING | string |
| primary_supplier_id | primary_supplier_id | STRING | string (FK) |
| alternate_supplier_id | alternate_supplier_id | STRING | string (FK, nullable) |
| stocking_location_id | stocking_location_id | STRING | string |
| on_hand_qty | on_hand_qty | STRING | decimal(18,2) |
| safety_stock_qty | safety_stock_qty | STRING | decimal(18,2) |
| avg_daily_demand | avg_daily_demand | STRING | decimal(18,2) |

#### purchase_orders.csv → bronze_purchase_orders_raw

| Raw field | Bronze column | Bronze type | Target type in Silver |
|---|---|---|---|
| po_id | po_id | STRING | string (PK) |
| po_line_number | po_line_number | STRING | integer (PK) |
| supplier_id | supplier_id | STRING | string (FK) |
| product_id | product_id | STRING | string (FK) |
| order_date | order_date | STRING | date |
| promised_delivery_date | promised_delivery_date | STRING | date |
| ordered_qty | ordered_qty | STRING | decimal(18,2) |
| unit_price | unit_price | STRING | decimal(18,4) |
| currency | currency | STRING | string |
| delivery_location_id | delivery_location_id | STRING | string |
| line_status | line_status | STRING | string |
| last_modified_ts | last_modified_ts | STRING | timestamp (UTC) |

#### shipment_events.json → bronze_shipment_events_raw

The nested structure is **preserved** (struct and array). It is flattened only in Silver. Leaf values are `STRING`. The original JSON line is kept in `raw_payload`.

| Raw JSON path | Bronze column | Bronze type | Notes |
|---|---|---|---|
| event_id | event_id | STRING | PK |
| shipment_id | shipment_id | STRING | |
| po_id | po_id | STRING | FK → purchase orders |
| carrier | carrier | STRING | |
| status | status | STRING | |
| event_ts | event_ts | STRING | Original UTC offset kept (e.g. `+01:00`) |
| location.location_id | location.location_id | STRUCT → STRING | |
| location.name | location.name | STRUCT → STRING | |
| location.country | location.country | STRUCT → STRING | |
| location.type | location.type | STRUCT → STRING | |
| eta_ts | eta_ts | STRING | Original UTC offset kept |
| items[].po_line_number | items[].po_line_number | ARRAY<STRUCT> → STRING | One element per PO line in the shipment |
| items[].product_id | items[].product_id | ARRAY<STRUCT> → STRING | |
| items[].quantity | items[].quantity | ARRAY<STRUCT> → STRING | |
| delay_reason_code | delay_reason_code | STRING | Nullable |
| *(whole line)* | raw_payload | STRING | Original line, byte-for-byte |

### 1.4 Columns added in Bronze

| Column | Type | Tables | Derivation | Purpose |
|---|---|---|---|---|
| `_rescued_data` | STRING | all | CSV: Auto Loader rescue column. JSON: JSON array of unexpected top-level keys | Schema-drift capture |
| `_corrupt_record` | STRING | shipment_events | Original line when it is not valid JSON | Malformed-record capture |
| `raw_payload` | STRING | shipment_events | Original NDJSON line | Full raw representation |
| `source_file_name` | STRING | all | `_metadata.file_path` | Lineage to file |
| `source_file_mod_ts` | TIMESTAMP | all | `_metadata.file_modification_time` | Identifies the file version |
| `ingestion_ts` | TIMESTAMP | all | `current_timestamp()` at load | Load time |
| `batch_id` | STRING | all | `<source>-<file mod time yyyyMMdd'T'HHmmss>-<first 8 chars of sha256(file path)>` | Deterministic batch id: the same file version always gets the same id |
| `source_system` | STRING | all | Constant per source (§1.2) | Source-system key |
| `record_hash` | STRING | all | `sha256` of all business columns + `_rescued_data` (+ `_corrupt_record` for JSON), joined with `\u0001`, NULL encoded as `<NULL>` | Row fingerprint for duplicate detection and change detection |
| `load_status` | STRING | all | See §1.5 | Per-record ingestion status |

### 1.5 Schema drift, malformed records and exclusions

| Situation | Handling | `load_status` |
|---|---|---|
| Row matches the contract | Loaded | `LOADED` |
| Extra column / type mismatch (CSV) or unexpected key (JSON) | Row kept; extra data goes to `_rescued_data` (`schemaEvolutionMode = rescue`: never fail, never evolve) | `SCHEMA_DRIFT` |
| JSON line cannot be parsed | Row kept; original text in `_corrupt_record` and `raw_payload` | `MALFORMED` |
| Missing contract column in a file | Column loaded as NULL; flagged by the `primary_key_present` expectation when it is a key | `LOADED` / expectation warning |
| `*_errors.csv`, `*_errors.json` (DQ answer keys) | **Never ingested**: filtered by file name; stored separately in `landing/_dq_expected` for Silver testing | – |

Bronze expectations only **warn** (rows are kept). Results are in `ops_pipeline_event_log`:
`no_schema_drift` (`_rescued_data IS NULL`), `primary_key_present` (key columns not NULL), `valid_json_line` (`_corrupt_record IS NULL`, JSON only).

### 1.6 Operational tables

| Table | Grain | Content |
|---|---|---|
| `ops_bronze_file_summary` | One file version per source | `rows_written`, `rows_loaded`, `rows_schema_drift`, `rows_malformed`, first/last `ingestion_ts`, `batch_id`, `target_table` |
| `ops_pipeline_event_log` | Pipeline event | Row counts per flow, expectation results, errors (Lakeflow event log) |

Reconciliation and lineage evidence: `databricks/pipelines/raw_to_bronze/notebooks/02_bronze_checks.py`.

---

## 2. Bronze → Silver

*To be completed in T31 with the Silver implementation.*

---

## 3. Silver → Gold

*To be completed in the Gold phase.*
