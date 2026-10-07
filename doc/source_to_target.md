# Source-to-Target Mapping – Supply Chain Disruption Analytics

Column-level lineage from the raw landing files to the Gold star schema, as implemented in
`databricks/pipelines/pipeline.sql` (one Lakeflow Declarative Pipeline, SQL).

| Section | Scope |
|---|---|
| 1. Raw → Bronze | Landing files → `bronze_*`, `ops_bronze_ingestion_log` |
| 2. Bronze → Silver | `bronze_*` → `silver_*`, `silver_quarantine`, `ops_dq_issues` |
| 3. Silver → Gold | `silver_*` → `gold_dim_*`, `gold_fact_*` |
| 4. Assumptions | Decisions that shape the mapping |

## Conventions

- **Location:** catalog `data2026acc123456`, schema `team4`. All layers share one schema, so the layer is a table-name prefix: `bronze_`, `silver_`, `gold_`, `ops_`.
- **Names:** source tables and columns keep the `doc/data_dictionary.md` names. Flattened JSON fields replace `.` with `_` (e.g. `location.country` → `location_country`).
- **Types:** Spark SQL types as written by the pipeline. Bronze is all `STRING`; typing happens in Silver.
- **(C)** marks a Confidential column of the data dictionary (`contract_lead_time_days`, `risk_rating`, `unit_cost`, `avg_daily_demand`, `unit_price`).
- **Lineage columns:** every Silver data table ends with `source_file_name`, `source_line_number`, `batch_id`, pointing to its Bronze row. They are not repeated in the column tables below.
- **Pipeline settings:**
  - `bronze.landing_root` = `/Volumes/data2026acc123456/team4/landing`
  - `gold.as_of_date` = the brief's `snapshot_date`; empty means the run date
  - session time zone `UTC`
- **Internal tables** (`TEMPORARY`, not published):
  - `suppliers_checked`, `products_checked`, `purchase_orders_typed`, `purchase_orders_checked`
  - `shipment_events_checked`, `shipment_event_lines_checked`
  - `po_line_kpis`
- **Row counts:**
  - They are the first load of the generated files in `data/`. They were reproduced by running `pipeline.sql` on those files.
  - The Gold KPIs of that run (snapshot 2026-10-05) match the reference values in `databricks/notebooks/3_gold_checks.py`.

## End-to-end lineage

| Raw file | Bronze | Silver | Gold |
|---|---|---|---|
| `suppliers.csv` | `bronze_suppliers` | `silver_suppliers` | `gold_dim_supplier`; supplier `risk_rating` → `risk_level_key` on the facts |
| `products.csv` | `bronze_products` | `silver_products` | `gold_dim_product`, `gold_fact_inventory_exposure`, stocking warehouses in `gold_dim_location` |
| `purchase_orders.csv` | `bronze_purchase_orders` | `silver_purchase_orders` | `gold_fact_purchase_order_line`, `gold_fact_inventory_exposure`, `gold_dim_date`, `gold_dim_location` |
| `shipment_events.json` | `bronze_shipment_events` | `silver_shipment_events` | `gold_fact_shipment_event`, PO-line derived fields, `gold_dim_location`, `gold_dim_date` |
| `fx_rates` seed (in `pipeline.sql`) | – | `silver_fx_rates` | `rate_to_eur` on `gold_fact_purchase_order_line` (via Silver) and `gold_fact_inventory_exposure` |
| Data dictionary value lists (in `pipeline.sql`) | – | – | `gold_dim_shipment_status`, `gold_dim_risk_level` |

| Layer | Rows (first load) |
|---|---|
| Raw → Bronze | 51 suppliers · 302 products · 4,020 PO lines · 9,431 events |
| Silver | 50 suppliers · 300 products · 3,911 PO lines · 22,933 event × PO lines (9,294 events) · 417 quarantined · 437 DQ issues |
| Gold | 3,798 PO-line facts · 9,294 event facts · 300 exposure facts |

---

## 1. Raw → Bronze

### 1.1 Principles

- **No business transformation.** Source values are kept as `STRING`, defects included (`EURO`, `-278.00`, `usd`), so they reach Silver unchanged.
- **CSV parsing:**
  - Columns are matched to the contract **by header name**, not by position.
  - Surrounding quotes are removed and `""` becomes `"`.
  - An empty field becomes NULL.
- **Streaming and idempotent.**
  - The tables are `STREAMING LIVE TABLE`s that read with `read_files(..., format => 'text', wholeText => true)`.
  - Each file is ingested once (checkpoint). A full refresh rebuilds Bronze from the landing Volume with identical `batch_id`s.
- **Line-level traceability.** Each file is read whole and split into lines, so every row carries its file and line number.
- **Grain / PK** of every Bronze source table: one non-empty data line of a source file. PK = (`batch_id`, `source_line_number`). Business keys can repeat; duplicates and corrections are kept.

### 1.2 Source → table mapping

| Raw file (landing folder) | Format | Bronze table | `source_system` | Load type | Business key | Rows |
|---|---|---|---|---|---|---|
| `landing/suppliers/*.csv` | CSV with header | `bronze_suppliers` | `ERP_MASTER_DATA` | Full snapshot per delivery, appended as a batch | `supplier_id` | 51 |
| `landing/products/*.csv` | CSV with header | `bronze_products` | `ERP_MASTER_DATA` | Full snapshot per delivery, appended as a batch | `product_id` | 302 |
| `landing/purchase_orders/*.csv` | CSV with header | `bronze_purchase_orders` | `ERP_PROCUREMENT` | Incremental by `last_modified_ts` | `po_id`, `po_line_number` | 4,020 |
| `landing/shipment_events/*.json` | NDJSON (one object per line) | `bronze_shipment_events` | `CARRIER_TMS` | Incremental by `event_ts` | `event_id` | 9,431 |

`fx_rates` is not a raw file. It is a seed inside `pipeline.sql` and enters the pipeline in Silver (§2.2).

### 1.3 Column mapping

**CSV sources:** every data dictionary field maps to a Bronze column **with the same name, type `STRING`**.

| Raw file | Bronze columns (all `STRING`) |
|---|---|
| `suppliers.csv` | `supplier_id`, `supplier_name`, `country_code`, `region`, `default_currency`, `contract_lead_time_days` (C), `risk_rating` (C), `is_active`, `onboarded_date` |
| `products.csv` | `product_id`, `product_name`, `category`, `unit_of_measure`, `unit_cost` (C), `cost_currency`, `criticality`, `primary_supplier_id`, `alternate_supplier_id`, `stocking_location_id`, `on_hand_qty`, `safety_stock_qty`, `avg_daily_demand` (C) |
| `purchase_orders.csv` | `po_id`, `po_line_number`, `supplier_id`, `product_id`, `order_date`, `promised_delivery_date`, `ordered_qty`, `unit_price` (C), `currency`, `delivery_location_id`, `line_status`, `last_modified_ts` |

**JSON source:** parsed with `from_json`. The nested structure is **preserved** and is flattened only in Silver.

| Raw JSON path | Bronze column | Bronze type |
|---|---|---|
| `event_id`, `shipment_id`, `po_id`, `carrier`, `status` | same names | `STRING` |
| `event_ts`, `eta_ts` | same names | `STRING` (original UTC offset kept, e.g. `+01:00`) |
| `location.{location_id, name, country, type}` | `location` | `STRUCT<location_id, name, country, type>` (all `STRING`) |
| `items[].{po_line_number, product_id, quantity}` | `items` | `ARRAY<STRUCT<po_line_number, product_id, quantity>>` (all `STRING`) |
| `delay_reason_code` | `delay_reason_code` | `STRING` (nullable) |

### 1.4 Columns added in Bronze (all four source tables)

| Column | Type | Derivation | Purpose |
|---|---|---|---|
| `_rescued_data` | STRING | CSV: JSON `{extra_columns, missing_columns}` when the header differs from the contract. JSON: array of unexpected top-level keys. NULL otherwise | Schema-drift response |
| `raw_payload` | STRING | The original source line | Original source representation |
| `source_file_name` | STRING | `_metadata.file_path` | Lineage to file |
| `source_line_number` | INT | Line number in the file (CSV header = line 1) | Lineage to line; first-occurrence dedup in Silver |
| `ingestion_ts` | TIMESTAMP | `current_timestamp()` at load (UTC) | Load time |
| `batch_id` | STRING | `<source>-<file modification time yyyyMMdd'T'HHmmss>-<first 8 chars of sha2(file path, 256)>` | One file version; deterministic |
| `source_system` | STRING | Constant per source (§1.2) | Source-system key |
| `record_hash` | STRING | `sha2(..., 256)` of all contract fields + `_rescued_data`, separator `chr(1)`, NULL encoded as `<NULL>`; JSON struct and array fields via `to_json` | Exact-copy detection in Silver |
| `load_status` | STRING | `LOADED`, `SCHEMA_DRIFT` or `MALFORMED` (§1.5) | Per-record ingestion status |

### 1.5 Schema drift, malformed records and exclusions

| Situation | Handling | `load_status` |
|---|---|---|
| Row matches the contract | Loaded | `LOADED` |
| Extra / missing CSV column, or unexpected JSON key | Row loaded; details in `_rescued_data` | `SCHEMA_DRIFT` |
| CSV row whose field count differs from the header, or a line that is not valid JSON | Row kept; original text in `raw_payload` | `MALFORMED` |
| Files ending `_errors.csv` / `_errors.json` (DQ answer keys) | Never ingested (filtered by file name). Stored in `landing/_dq_expected/` for the Silver checks | – |

Bronze expectations only **warn**; rows are kept. Results are in the pipeline event log `ops_pipeline_event_log`:

| Expectation | Condition |
|---|---|
| `no_schema_drift` | `_rescued_data IS NULL` |
| `primary_key_present` | business key not null |
| `parseable_record` | `load_status <> 'MALFORMED'` |

### 1.6 `ops_bronze_ingestion_log`

Source: `UNION ALL` of the four Bronze tables, grouped per batch. Grain / PK: one ingested file version (`batch_id`). 4 rows.

| Column | Type | Derivation |
|---|---|---|
| `batch_id`, `source_file_name`, `source_system` | STRING | Group-by keys |
| `input_count` | BIGINT | `COUNT(1)` of data lines |
| `written_count` | BIGINT | Lines with `load_status <> 'MALFORMED'` |
| `rejected_count` | BIGINT | Lines with `load_status = 'MALFORMED'` (kept in Bronze, excluded in Silver) |
| `schema_drift_count` | BIGINT | Lines with `load_status = 'SCHEMA_DRIFT'` |
| `ingestion_ts` | TIMESTAMP | `MAX(ingestion_ts)` of the batch |
| `target_table` | STRING | Bronze table name (constant per branch of the union) |

Duration and failure details are in `ops_pipeline_event_log`.

---

## 2. Bronze → Silver

### 2.1 Common rules

**Scope**

| Source | Bronze rows read |
|---|---|
| suppliers, products | Latest batch only (`batch_id = max(batch_id)`): full snapshot, SCD Type 1 |
| purchase_orders, shipment_events | All batches |

**Typing**

- `nullif(trim(x), '')`: values are trimmed and an empty string becomes NULL.
- Codes and IDs are upper-cased.
- Casts use `try_cast` / `try_to_timestamp`: a value that cannot be converted becomes NULL and raises `invalid_type`. It never fails the pipeline.

**Dispositions.** Every rule hit is a `named_struct(rule, action, column_name, description)` in the `dq_issues` array of the `*_checked` table.

| Action | Effect |
|---|---|
| `QUARANTINE` | Row excluded from the Silver table and written to `silver_quarantine` |
| `NULLIFY` | Row kept, offending value set to NULL |
| `FIX` | Row kept, value standardised (only: valid ISO currency in lower case → upper case) |
| `FLAG` | Row kept unchanged |

A Silver table keeps only the rows where `NOT exists(dq_issues, x -> x.action = 'QUARANTINE')`. Every hit, whatever its action, is written to `ops_dq_issues`.

**Bronze status**

| Bronze `load_status` | Silver rule | Action |
|---|---|---|
| `MALFORMED` | `malformed_record` | QUARANTINE |
| `SCHEMA_DRIFT` | `schema_drift` | FLAG |

**Valid currencies:** EUR, USD, GBP, CHF, CNY, INR, JPY, KRW, MXN, PLN, TWD, VND, SEK, NOK, DKK, CZK, HUF, RON, TRY, CAD, AUD, BRL, SGD, HKD, THB, MYR, IDR, ZAR, AED, SAR. `XXX` and symbols such as `€` are not accepted.

### 2.2 `silver_fx_rates`

Source: `VALUES` seed in `pipeline.sql`. Grain / PK: (`currency`, `valid_from`). 10 rows. **The rate values are placeholders.**

| Column | Type | Derivation |
|---|---|---|
| `currency` | STRING | seed |
| `valid_from` | DATE | seed (EUR from 2000-01-01, all others from 2025-01-01) |
| `rate_to_eur` | DECIMAL(18,8) | `CAST(CAST(rate AS DOUBLE) AS DECIMAL(18,8))`, EUR per 1 unit of currency |

A rate applies from `valid_from` until the next `valid_from` of the same currency (`lead(valid_from)`).

### 2.3 `bronze_suppliers` → `silver_suppliers`

Via `suppliers_checked`. Scope: latest batch. Grain / PK: `supplier_id`. 51 → **50** rows.

| Silver column | Type | Bronze column | Transformation | DQ rule → action |
|---|---|---|---|---|
| `supplier_id` | STRING | `supplier_id` | trim, upper | empty → `missing_required` QUARANTINE; not first line of the key → `duplicate_supplier` QUARANTINE |
| `supplier_name` | STRING | `supplier_name` | trim | empty → `missing_required` FLAG |
| `country_code` | STRING | `country_code` | trim, upper | not `^[A-Z]{2}$` → `invalid_domain` FLAG |
| `region` | STRING | `region` | trim, upper | not EMEA/APAC/AMER → `invalid_domain` FLAG |
| `default_currency` | STRING | `default_currency` | upper; kept only if a valid currency | lower-case valid code → `currency_inconsistency` FIX; not valid → `currency_inconsistency` NULLIFY |
| `contract_lead_time_days` (C) | INT | `contract_lead_time_days` | `try_cast INT`; NULL if ≤ 0 | not an integer → `invalid_type` NULLIFY; ≤ 0 → `invalid_value` NULLIFY |
| `risk_rating` (C) | STRING | `risk_rating` | trim, upper | not LOW/MEDIUM/HIGH → `invalid_domain` FLAG |
| `is_active` | BOOLEAN | `is_active` | true/1/yes/y → true; false/0/no/n → false; else NULL | `invalid_type` NULLIFY |
| `onboarded_date` | DATE | `onboarded_date` | `try_cast DATE` | `invalid_type` NULLIFY |

Fail expectation: `supplier_id IS NOT NULL`.

### 2.4 `bronze_products` → `silver_products`

Via `products_checked`. Scope: latest batch, validated against `silver_suppliers`. Grain / PK: `product_id`. 302 → **300** rows.

| Silver column | Type | Bronze column | Transformation | DQ rule → action |
|---|---|---|---|---|
| `product_id` | STRING | `product_id` | trim, upper | empty → `missing_required` QUARANTINE; not first line of the key → `duplicate_product` QUARANTINE |
| `product_name` | STRING | `product_name` | trim | empty → `missing_required` FLAG |
| `category` | STRING | `category` | trim | not Raw Materials / Packaging / Electronic Components / Fasteners / Mechanical Parts / Chemicals → `invalid_domain` FLAG |
| `unit_of_measure` | STRING | `unit_of_measure` | trim, upper | not EA/KG/L → `invalid_domain` FLAG |
| `unit_cost` (C) | DECIMAL(18,4) | `unit_cost` | `try_cast`; NULL if < 0 | `invalid_type` NULLIFY; < 0 → `negative_price` NULLIFY |
| `cost_currency` | STRING | `cost_currency` | upper; kept only if a valid currency | `currency_inconsistency` FIX (case) / NULLIFY (not valid) |
| `criticality` | STRING | `criticality` | trim, upper | not CRITICAL/HIGH/MEDIUM/LOW → `invalid_domain` FLAG |
| `primary_supplier_id` | STRING | `primary_supplier_id` | trim, upper | not in `silver_suppliers` → `unknown_supplier` FLAG (row kept) |
| `alternate_supplier_id` | STRING | `alternate_supplier_id` | trim, upper; NULL if not in `silver_suppliers` | `unknown_supplier` NULLIFY |
| `stocking_location_id` | STRING | `stocking_location_id` | trim, upper | – |
| `on_hand_qty` | DECIMAL(18,2) | `on_hand_qty` | `try_cast`; NULL if < 0 | `invalid_type` / `negative_quantity` NULLIFY |
| `safety_stock_qty` | DECIMAL(18,2) | `safety_stock_qty` | `try_cast`; NULL if < 0 | `invalid_type` / `negative_quantity` NULLIFY |
| `avg_daily_demand` (C) | DECIMAL(18,2) | `avg_daily_demand` | `try_cast`; NULL if < 0 | `invalid_type` / `negative_quantity` NULLIFY |

Fail expectation: `product_id IS NOT NULL`.

### 2.5 `bronze_purchase_orders` → `silver_purchase_orders`

Two internal steps:

1. **`purchase_orders_typed`**: every PO-line version from all batches, typed. It is also the target of the shipment-event foreign keys (§2.6).
2. **`purchase_orders_checked`**: resolves versions, then validates against `silver_suppliers`, `silver_products` and `silver_fx_rates`.

Grain / PK: (`po_id`, `po_line_number`), current version. 4,020 → **3,911** rows.

**Version resolution** (before the record checks):

1. Only the versions with the highest `last_modified_ts` per key are kept (`dense_rank`). Older versions are superseded; this is normal, not an issue.
2. Among those versions, the first by (`batch_id`, `source_line_number`) wins.
3. An identical record (same `record_hash`) re-sent in a later file is dropped silently.
4. Any other copy → `duplicate_order_line` QUARANTINE.

| Silver column | Type | Bronze column | Transformation | DQ rule → action |
|---|---|---|---|---|
| `po_id` | STRING | `po_id` | trim, upper | empty → `missing_required` QUARANTINE |
| `po_line_number` | INT | `po_line_number` | `try_cast INT` | `missing_required` / `invalid_type` QUARANTINE |
| `supplier_id` | STRING | `supplier_id` | trim, upper | empty → `missing_required` QUARANTINE; not in `silver_suppliers` → `unknown_supplier` QUARANTINE |
| `product_id` | STRING | `product_id` | trim, upper | empty → `missing_required` QUARANTINE; not in `silver_products` → `unknown_product` QUARANTINE |
| `order_date` | DATE | `order_date` | `try_cast DATE` | `missing_required` / `invalid_type` QUARANTINE |
| `promised_delivery_date` | DATE | `promised_delivery_date` | `try_cast DATE` | `invalid_type` QUARANTINE; before `order_date` → `delivery_before_order` QUARANTINE |
| `ordered_qty` | DECIMAL(18,2) | `ordered_qty` | `try_cast` | `missing_required` / `invalid_type` QUARANTINE; < 0 → `negative_quantity` QUARANTINE; = 0 → `invalid_value` QUARANTINE |
| `unit_price` (C) | DECIMAL(18,4) | `unit_price` | `try_cast` | `invalid_type` QUARANTINE; < 0 → `negative_price` QUARANTINE |
| `currency` | STRING | `currency` | upper; kept only if a valid currency | see the `currency` rules below |
| `rate_to_eur` | DECIMAL(18,8) | *(derived)* | `silver_fx_rates` rate of `currency` valid on `order_date` | no rate → `missing_fx_rate` QUARANTINE |
| `delivery_location_id` | STRING | `delivery_location_id` | trim, upper | – |
| `line_status` | STRING | `line_status` | trim, upper | not OPEN/PARTIALLY_RECEIVED/CLOSED/CANCELLED → `invalid_domain` FLAG |
| `last_modified_ts` | TIMESTAMP | `last_modified_ts` | `try_to_timestamp(..., "yyyy-MM-dd'T'HH:mm:ssXXX")` → UTC | `invalid_type` FLAG |

`currency` rules (one row can hit more than one):

| Condition | Rule | Action |
|---|---|---|
| Empty | `missing_required` | QUARANTINE |
| Valid code in lower case (e.g. `usd`) | `currency_inconsistency` | FIX (upper-cased) |
| Not a valid code (e.g. `XXX`, `€`) | `currency_inconsistency` | QUARANTINE |
| Differs from the supplier's `default_currency` | `currency_inconsistency` | FLAG |

Fail expectations: key not null; `ordered_qty > 0`; `rate_to_eur IS NOT NULL`.
CANCELLED lines **stay** in Silver; Gold excludes them (§3.3).

### 2.6 `bronze_shipment_events` → `silver_shipment_events`

Scope: all batches, validated against `purchase_orders_typed` (all typed PO-line versions, before quarantine) and against `silver_products`.
Grain / PK: one event × PO line (`items[]` exploded); PK (`event_id`, `po_line_number`). 9,431 events → **22,933** rows from 9,294 events.

The checks run in two internal steps.

**Step 1: `shipment_events_checked`** (event level, one row per JSON line). A failure quarantines the whole event.

- Duplicate `event_id`: the first by (`batch_id`, `source_line_number`) is kept. An identical copy in another file is dropped silently. Any other copy → `duplicate_event` QUARANTINE.
- `invalid_status_sequence` (QUARANTINE), evaluated only on events not already quarantined:
  - a DELIVERED event before the shipment's first PICKED_UP
  - any event after the shipment's first valid DELIVERED

**Step 2: `shipment_event_lines_checked`** (line level, one row per event × `items[]` element). A failure quarantines only that event × line.

- `silver_shipment_events` keeps the lines with no QUARANTINE hit **and** whose event is not blocked.

| Silver column | Type | Bronze source | Transformation | DQ rule → action |
|---|---|---|---|---|
| `event_id` | STRING | `event_id` | trim, upper | empty → `missing_required` QUARANTINE |
| `shipment_id` | STRING | `shipment_id` | trim, upper | empty → `missing_required` QUARANTINE |
| `po_id` | STRING | `po_id` | trim, upper | empty → `missing_required` QUARANTINE; not in `purchase_orders_typed` → `unknown_purchase_order` QUARANTINE |
| `carrier` | STRING | `carrier` | trim | – |
| `status` | STRING | `status` | trim, upper | see the `status` rules below |
| `event_ts` | TIMESTAMP | `event_ts` | `try_to_timestamp(..., "yyyy-MM-dd'T'HH:mm:ssXXX")` → UTC | `missing_required` / `invalid_type` QUARANTINE |
| `location_id` | STRING | `location.location_id` | trim, upper | – |
| `location_name` | STRING | `location.name` | trim | – |
| `location_country` | STRING | `location.country` | trim, upper | – |
| `location_type` | STRING | `location.type` | trim, upper | not WAREHOUSE/PORT/HUB/SUPPLIER_SITE → `invalid_domain` FLAG |
| `eta_ts` | TIMESTAMP | `eta_ts` | ISO 8601 with offset → UTC | `invalid_type` NULLIFY |
| `po_line_number` | INT | `items[].po_line_number` | `explode`, `try_cast INT` | `missing_required` / `invalid_type` QUARANTINE (line); (`po_id`, `po_line_number`) not in `purchase_orders_typed` → `unknown_order_line` QUARANTINE (line) |
| `product_id` | STRING | `items[].product_id` | `explode`, trim, upper | empty → `missing_required` QUARANTINE (line); not in `silver_products` → `unknown_product` QUARANTINE (line) |
| `quantity` | DECIMAL(18,2) | `items[].quantity` | `explode`, `try_cast` | `missing_required` / `invalid_type` QUARANTINE (line); < 0 → `negative_quantity` QUARANTINE (line) |
| `delay_reason_code` | STRING | `delay_reason_code` | trim, upper | not CUSTOMS_HOLD / WEATHER / CARRIER_CAPACITY / VESSEL_DELAY / PORT_CONGESTION / SUPPLIER_CAPACITY / MATERIAL_SHORTAGE / DOCUMENTATION → `invalid_domain` FLAG |
| *(event)* | – | `items` | – | empty or missing → `missing_required` QUARANTINE |

`status` rules:

| Condition | Rule | Action |
|---|---|---|
| Empty | `missing_required` | QUARANTINE |
| Not one of the 8 dictionary values | `invalid_domain` | QUARANTINE |
| DELIVERED before the earliest `order_date` of the PO | `delivery_before_order` | QUARANTINE |

Fail expectations: key not null; `event_ts IS NOT NULL`; `quantity >= 0`.

### 2.7 Quality outputs

Both tables are `UNION ALL`s over the five `*_checked` tables. Line-level rows of an event that was already quarantined at event level are not repeated in `silver_quarantine`.

**`silver_quarantine`**: one blocking rule hit on one record (`explode(filter(dq_issues, x -> x.action = 'QUARANTINE'))`). **417** rows.

| Column | Type | Derivation |
|---|---|---|
| `entity` | STRING | `suppliers`, `products`, `purchase_orders`, `shipment_events` |
| `record_key` | STRING | `supplier_id`; `product_id`; `concat_ws('#', po_id, po_line_number)`; `event_id` (event level); `concat_ws('#', event_id, po_line_number)` (line level) |
| `rule` | STRING | `dq_issues.rule` |
| `reason` | STRING | `dq_issues.description` (with the offending value) |
| `raw_payload` | STRING | Bronze `raw_payload` (for event lines: the whole event line) |
| `source_file_name`, `source_line_number`, `batch_id` | STRING, INT, STRING | Lineage to Bronze |
| `processing_ts` | TIMESTAMP | `current_timestamp()` |

**`ops_dq_issues`**: one rule hit on one column of one record, for every action (`explode(dq_issues)`). **437** rows: the 417 quarantine hits + 20 NULLIFY / FIX / FLAG hits.

| Column | Type | Derivation |
|---|---|---|
| `entity`, `record_key` | STRING | As in `silver_quarantine` |
| `rule`, `action`, `column_name` | STRING | `dq_issues` struct fields |
| `reason` | STRING | `dq_issues.description` |
| `source_file_name`, `source_line_number`, `batch_id` | STRING, INT, STRING | Lineage to Bronze |
| `processing_ts` | TIMESTAMP | `current_timestamp()` |

### 2.8 Injected defects → Silver rule

The rule names match the `dq_rule` values of the `*_errors.csv` answer keys. Detection is checked in `databricks/notebooks/2_silver_checks.py` against `landing/_dq_expected/`.

| Entity | Injected defect (`dq_rule`) | Answer key | Silver action |
|---|---|---|---|
| suppliers | `currency_inconsistency` | 1 | NULLIFY (`EURO` is not valid) |
| suppliers | `duplicate_supplier` | 1 | QUARANTINE |
| products | `negative_quantity` | 2 | NULLIFY |
| products | `unknown_supplier` | 2 | NULLIFY (both on `alternate_supplier_id`) |
| products | `currency_inconsistency` | 2 | FIX (`usd` → `USD`) |
| products | `duplicate_product` | 2 | QUARANTINE |
| purchase_orders | `unknown_supplier`, `unknown_product`, `delivery_before_order`, `negative_quantity`, `duplicate_order_line` | 20 each | QUARANTINE |
| purchase_orders | `currency_inconsistency` | 20 | 9 not valid (`XXX`, `€`) → QUARANTINE; 9 differ from the supplier default → FLAG; 2 `usd` → FIX, plus FLAG because USD also differs from the supplier default (EUR) |
| shipment_events | `unknown_product` | 153 lines | QUARANTINE (line) |
| shipment_events | `negative_quantity` | 38 lines | QUARANTINE (line) |
| shipment_events | `invalid_status_sequence`, `duplicate_event`, `delivery_before_order` | 38 each | QUARANTINE (event) |

---

## 3. Silver → Gold

### 3.1 Common rules

| Topic | Rule |
|---|---|
| Surrogate keys | `<entity>_key = xxhash64(concat_ws('\|\|', natural key))` (BIGINT), deterministic across refreshes. A NULL natural key maps to `-1`. |
| Unknown member | Every dimension has one row with key `-1` (`UNKNOWN` / `Unknown …`). |
| Date keys | `coalesce(CAST(date_format(date, 'yyyyMMdd') AS INT), -1)` → `gold_dim_date.date_key` |
| Snapshot date | `COALESCE(try_cast('${gold.as_of_date}' AS DATE), current_date())`, written `snap` below |
| Event dates | `to_date(event_ts)` in UTC (Silver already converted the offsets) |
| Table type | Live tables recomputed on each update. Every column has a comment with its sensitivity label (`[Internal]`, `[Public]`, `[Confidential]`). Key columns have `ON VIOLATION FAIL UPDATE` expectations. |

### 3.2 Dimensions

#### `gold_dim_date`

Source: all dates in `silver_purchase_orders` (`order_date`, `promised_delivery_date`) and `silver_shipment_events` (`to_date(event_ts)`, `to_date(eta_ts)`), plus `snap`. The calendar is `sequence()` from 1 January of the earliest year to 31 December of the latest year. Grain: one calendar day. **731** rows (2025–2026 + Unknown).

| Gold column | Type | Derivation |
|---|---|---|
| `date_key` | INT | `yyyyMMdd(date)`; `-1` = Unknown |
| `date` | DATE | generated calendar day (NULL for -1) |
| `year`, `quarter`, `month` | INT | `year()`, `quarter()`, `month()` |
| `month_name` | STRING | `date_format(date, 'MMMM')`; `Unknown` for -1 |

#### `gold_dim_supplier`

Source: `silver_suppliers`. Grain: one supplier. **51** rows (50 + Unknown).

| Gold column | Type | Silver source | Derivation |
|---|---|---|---|
| `supplier_key` | BIGINT | `supplier_id` | `xxhash64`; Unknown row: `-1`, `UNKNOWN`, `Unknown supplier` |
| `supplier_id`, `supplier_name`, `country_code`, `region`, `default_currency` | STRING | same names | 1:1 |
| `contract_lead_time_days` (C) | INT | same name | 1:1 |
| `risk_rating` (C) | STRING | same name | 1:1 |
| `is_active` | BOOLEAN | same name | 1:1 |
| `onboarded_date` | DATE | same name | 1:1 |

#### `gold_dim_product`

Source: `silver_products`, left-joined to `silver_suppliers` on `alternate_supplier_id`. Grain: one product. **301** rows (300 + Unknown).

| Gold column | Type | Silver source | Derivation |
|---|---|---|---|
| `product_key` | BIGINT | `product_id` | `xxhash64`; Unknown row: `-1`, `UNKNOWN`, `Unknown product` |
| `product_id`, `product_name`, `category`, `unit_of_measure`, `criticality`, `primary_supplier_id`, `alternate_supplier_id`, `stocking_location_id` | STRING | same names | 1:1 |
| `has_active_alternate` | BOOLEAN | `alternate_supplier_id`, `primary_supplier_id`, alternate supplier's `is_active` | `alternate_supplier_id IS NOT NULL AND alternate_supplier_id <> primary_supplier_id AND coalesce(is_active, false)` |

#### `gold_dim_location`

Grain: one location. **78** rows (77 + Unknown). Sources:

- `seen`: locations of `silver_shipment_events`, with the attributes of the most recent event per `location_id`.
- `referenced`: warehouse IDs that appear only in `silver_purchase_orders.delivery_location_id` or `silver_products.stocking_location_id`. These rows have NULL name, country and type.

| Gold column | Type | Silver source | Derivation |
|---|---|---|---|
| `location_key` | BIGINT | `location_id` | `xxhash64`; Unknown row: `-1`, `UNKNOWN`, `Unknown location` |
| `location_id` | STRING | event `location_id` ∪ `delivery_location_id` ∪ `stocking_location_id` | distinct |
| `location_name`, `location_country`, `location_type` | STRING | `silver_shipment_events` | latest event per location; NULL for warehouse-only IDs |

#### `gold_dim_shipment_status`

Source: `VALUES` of the 8 data dictionary statuses. **9** rows (8 + Unknown).

| Gold column | Type | Derivation |
|---|---|---|
| `shipment_status_key` | BIGINT | `xxhash64(status)` |
| `status` | STRING | CREATED, PICKED_UP, IN_TRANSIT, CUSTOMS, OUT_FOR_DELIVERY, DELIVERED, DELAYED, CANCELLED; `UNKNOWN` for -1 |

#### `gold_dim_risk_level`

Source: `VALUES` LOW, MEDIUM, HIGH, i.e. the supplier `risk_rating` values (assumption, see §4). **4** rows (3 + Unknown).

| Gold column | Type | Derivation |
|---|---|---|
| `risk_level_key` | BIGINT | `xxhash64(risk_level)` |
| `risk_level` | STRING | LOW, MEDIUM, HIGH; `UNKNOWN` for -1 |

### 3.3 `po_line_kpis` – PO-line derived fields (brief §4)

This internal table is computed once and used by `gold_fact_purchase_order_line` and `gold_fact_inventory_exposure`.

- **Input:** `silver_purchase_orders` where `line_status <> 'CANCELLED'`, left-joined to the aggregate of `silver_shipment_events` per (`po_id`, `po_line_number`).
- **Rows:** 3,911 − 113 cancelled = **3,798**.

| Field | Derivation |
|---|---|
| `received_qty` | `SUM(quantity)` of DELIVERED event lines; 0 if none |
| `received_by_promised_qty` | `SUM(quantity)` of DELIVERED event lines with `to_date(event_ts) <= promised_delivery_date` (internal) |
| `actual_delivery_date` | `MAX(to_date(event_ts))` of DELIVERED event lines |
| `latest_eta_date` | `max_by(to_date(eta_ts), event_ts)`: the ETA of the most recent event containing the line |
| `past_due`, `late_eta` | `promised_delivery_date <= snap`; `latest_eta_date > promised_delivery_date` (internal) |
| `is_delivered` | `received_qty >= ordered_qty OR line_status = 'CLOSED'` |
| `delay_days` | delivered: `datediff(actual_delivery_date, promised_delivery_date)`; past due: `datediff(snap, promised_delivery_date)`; otherwise 0 |
| `is_on_time` | `is_delivered AND actual_delivery_date <= promised_delivery_date` |
| `is_delayed` | `(is_delivered AND delay_days > 0) OR (NOT is_delivered AND (past_due OR late_eta))` |
| `delayed_qty` | past due: `greatest(ordered_qty − received_by_promised_qty, 0)`; else late ETA: `greatest(ordered_qty − received_qty, 0)`; otherwise 0 |

### 3.4 Facts

#### `gold_fact_purchase_order_line`

Source: `po_line_kpis`, left-joined to `silver_suppliers` for the risk rating. Grain / PK: (`po_id`, `po_line_number`). **3,798** rows. `CLUSTER BY (order_date_key, supplier_key)`.

| Gold column | Type | Source | Derivation |
|---|---|---|---|
| `po_id` | STRING | `po_id` | 1:1 |
| `po_line_number` | INT | `po_line_number` | 1:1 |
| `supplier_key` | BIGINT | `supplier_id` | `xxhash64` → `gold_dim_supplier` |
| `product_key` | BIGINT | `product_id` | `xxhash64` → `gold_dim_product` |
| `location_key` | BIGINT | `delivery_location_id` | `xxhash64` → `gold_dim_location` |
| `risk_level_key` | BIGINT | `silver_suppliers.risk_rating` | `xxhash64` → `gold_dim_risk_level` |
| `order_date_key` | INT | `order_date` | date key (active relationship) |
| `promised_delivery_date_key` | INT | `promised_delivery_date` | date key (role-playing, inactive) |
| `actual_delivery_date_key` | INT | `actual_delivery_date` | date key; -1 = not delivered |
| `latest_eta_date_key` | INT | `latest_eta_date` | date key; -1 = no shipment event |
| `line_status` | STRING | `line_status` | 1:1 (OPEN, PARTIALLY_RECEIVED, CLOSED) |
| `ordered_qty` | DECIMAL(18,2) | `ordered_qty` | 1:1 |
| `unit_price` (C) | DECIMAL(18,4) | `unit_price` | 1:1 (line currency) |
| `rate_to_eur` | DECIMAL(18,8) | `rate_to_eur` | 1:1 (valid on `order_date`) |
| `received_qty`, `delay_days`, `delayed_qty`, `is_delivered`, `is_on_time`, `is_delayed` | DECIMAL / INT / BOOLEAN | `po_line_kpis` | §3.3 |

Fail expectations: `supplier_key` and `product_key` not null; no CANCELLED line; `delayed_qty >= 0`; `is_on_time` implies `is_delivered`.

#### `gold_fact_shipment_event`

Source: `SELECT DISTINCT` of the event-level columns of `silver_shipment_events`, which are the same on all lines of an event. The supplier is `MIN(supplier_id)` of the PO's lines in `silver_purchase_orders`. Grain / PK: `event_id`. 22,933 → **9,294** rows. `CLUSTER BY (event_date_key, supplier_key)`.

| Gold column | Type | Silver source | Derivation |
|---|---|---|---|
| `event_id`, `shipment_id`, `po_id` | STRING | same names | 1:1 (distinct event) |
| `shipment_status_key` | BIGINT | `status` | `xxhash64` → `gold_dim_shipment_status` |
| `supplier_key` | BIGINT | `silver_purchase_orders.supplier_id` (via `po_id`) | `xxhash64(MIN(supplier_id))`; -1 if the PO has no Silver line |
| `location_key` | BIGINT | `location_id` | `xxhash64` → `gold_dim_location` |
| `event_date_key` | INT | `event_ts` | date key of `to_date(event_ts)` |
| `carrier`, `delay_reason_code` | STRING | same names | 1:1 |
| `event_ts`, `eta_ts` | TIMESTAMP | same names | 1:1 (UTC) |

Fail expectation: `event_id` and `shipment_status_key` not null.

#### `gold_fact_inventory_exposure`

Grain / PK: (`snapshot_date_key`, `product_key`); only the current snapshot is kept. **300** rows. Sources:

- `silver_products`, left-joined to:
  - `inbound` = `SUM(delayed_qty)` of open delayed lines (`po_line_kpis` where `NOT is_delivered AND is_delayed`), per product
  - `earliest` = the product's open delayed line with the earliest `promised_delivery_date` (then `po_id`, `po_line_number`), with its supplier's `contract_lead_time_days`
  - `silver_suppliers` on `primary_supplier_id` (risk)
  - `silver_fx_rates` on `cost_currency`, rate valid on `snap`

| Gold column | Type | Source | Derivation |
|---|---|---|---|
| `snapshot_date_key` | INT | `snap` | date key |
| `product_key` | BIGINT | `product_id` | `xxhash64` → `gold_dim_product` |
| `supplier_key` | BIGINT | `primary_supplier_id` | `xxhash64` → `gold_dim_supplier` |
| `location_key` | BIGINT | `stocking_location_id` | `xxhash64` → `gold_dim_location` |
| `risk_level_key` | BIGINT | primary supplier's `risk_rating` | `xxhash64` → `gold_dim_risk_level` |
| `on_hand_qty`, `safety_stock_qty` | DECIMAL(18,2) | `silver_products` | 1:1 |
| `avg_daily_demand` (C) | DECIMAL(18,2) | `silver_products` | 1:1 |
| `unit_cost` (C) | DECIMAL(18,4) | `silver_products` | 1:1 (in `cost_currency`) |
| `rate_to_eur` | DECIMAL(18,8) | `silver_fx_rates` | rate of `cost_currency` valid on `snap` |
| `days_of_cover` | DOUBLE | stock + demand | `round(greatest(on_hand_qty − safety_stock_qty, 0) / avg_daily_demand, 2)`; NULL if demand ≤ 0 or a stock figure is NULL |
| `delayed_inbound_qty` | DECIMAL(18,2) | `inbound` | `coalesce(SUM(delayed_qty), 0)` |
| `days_until_arrival` | INT | `earliest` | `coalesce(datediff(latest_eta_date, snap), delay_days + contract_lead_time_days)`; NULL if the product has no open delayed line |

Fail expectations: `product_key` not null; `days_of_cover IS NULL OR days_of_cover >= 0`.

### 3.5 KPI → Gold columns

The 8 KPIs are DAX measures in the semantic model, built on these columns. Reference values are for snapshot 2026-10-05 with the placeholder FX rates.

| KPI | Gold table | Columns used | Reference value |
|---|---|---|---|
| Order Value | `gold_fact_purchase_order_line` | `ordered_qty × unit_price × rate_to_eur` | 190,388,429.60 |
| On-Time Delivery Rate | `gold_fact_purchase_order_line` | `is_on_time` / `is_delivered` | 0.6446 |
| Average Delay (days) | `gold_fact_purchase_order_line` | `delay_days` where `is_delivered AND delay_days > 0` | 6.74 |
| Supplier Fill Rate | `gold_fact_purchase_order_line` | `MIN(received_qty, ordered_qty)` / `ordered_qty` where `promised_delivery_date_key <= snapshot` | 0.8426 |
| Exposed Value | `gold_fact_inventory_exposure` | `delayed_inbound_qty × unit_cost × rate_to_eur` where `days_of_cover < days_until_arrival` | 2,574,955.37 |
| Delayed Quantity | `gold_fact_purchase_order_line` | `SUM(delayed_qty)` | 6,929,712.71 |
| Critical-Item Count | `gold_fact_inventory_exposure` + `gold_dim_product` | distinct `product_key` with `criticality = 'CRITICAL'` and `delayed_inbound_qty > 0` | 22 |
| Alternate-Supplier Coverage | `gold_dim_product` | `has_active_alternate` among `criticality = 'CRITICAL'` | 0.5862 |

---

## 4. Assumptions that shape the mapping

These are points the business brief and data dictionary leave open; the pipeline takes the position below.

| # | Topic | Mapping decision |
|---|---|---|
| 1 | FX rate values | `silver_fx_rates` uses placeholder rates; every EUR figure depends on them |
| 2 | FX for `unit_cost` | Rate valid on the snapshot date (a product has no `order_date`) |
| 3 | `gold_dim_risk_level` | Supplier `risk_rating`; product `criticality` would be the alternative |
| 4 | "Earliest delayed open line" | Earliest `promised_delivery_date`, then `po_id`, `po_line_number` |
| 5 | Lead time without ETA | `contract_lead_time_days` of that line's supplier |
| 6 | Cancelled lines | Kept in `silver_purchase_orders`, excluded from Gold (113 lines) |
| 7 | Flattened JSON names | `location.*` → `location_*`; `items[].*` → `po_line_number`, `product_id`, `quantity` |
| 8 | Supplier of a shipment event | `MIN(supplier_id)` among the PO's Silver lines; -1 if none |
| 9 | Events with no valid line | No Silver / Gold row; their lines are in `silver_quarantine` (23 events) |
| 10 | Locations without events | In `gold_dim_location` with NULL name, country and type |
| 11 | Reference data history | Suppliers and products are SCD Type 1 (latest batch); history stays in Bronze |
| 12 | Snapshot history | Only the current snapshot is kept in `gold_fact_inventory_exposure` |
