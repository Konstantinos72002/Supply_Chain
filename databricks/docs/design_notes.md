# Design notes – Team 4 Supply Chain Disruption Analytics

Documentation required by the assignment (§4 refresh strategy and source-to-target mapping, §6.2 slowly
changing and late-arriving data) plus the assumptions taken where the documents are silent.
The Gold model is documented in `data_model.md` (generated from the code).

## 1. Refresh strategy

One Lakeflow ETL pipeline (`team4-supply-chain-medallion`), three source files, one dependency graph.

| Layer | Source file | Mechanism | Rerun behaviour |
|---|---|---|---|
| Bronze | `medallion/01_bronze.py` | Auto Loader streaming tables; one file = one batch | each file read once (checkpoint); *Full refresh* rebuilds from the landing Volume with identical `batch_id`s |
| Silver | `medallion/02_silver.py` | materialized views over all Bronze rows | deterministic recomputation: same Bronze → same Silver |
| Gold | `medallion/03_gold.py` | materialized views over Silver | recomputed; `gold.as_of_date` = brief `snapshot_date` (empty = today) |

Fail-expectations stop an update before inconsistent data reaches Gold. Duration and failures are in the
pipeline event log `ops_pipeline_event_log`; counts per batch in `ops_bronze_ingestion_log`.

## 2. Slowly changing reference data (suppliers, products)

Full extract per batch (brief §5). Bronze keeps every delivery; Silver uses the **latest batch** (SCD Type 1:
current attributes overwrite). Supplier `risk_rating`, `is_active` and product `criticality` are therefore
reported as of the latest extract, also for past orders. History stays in Bronze.

## 3. Late-arriving and corrected records

| Case | Treatment |
|---|---|
| Corrected PO line (newer `last_modified_ts`) | latest version wins (data dictionary); older versions stay in Bronze |
| Same PO line re-sent unchanged in a later file | dropped silently (overlapping delivery) |
| Conflicting versions with the same `last_modified_ts` | first occurrence (batch, line) kept; others quarantined as `duplicate_order_line` |
| Late shipment event | events ordered by `event_ts`, not arrival; lifecycle checks re-evaluated on every refresh |
| Transactions referencing master data not yet received | quarantined (`unknown_supplier` / `unknown_product`); released automatically by the refresh after the master data arrives |

## 4. Source-to-target mapping

**Bronze** – all source fields as strings, unchanged, plus the brief §5 audit columns (`source_file_name`,
`ingestion_ts`, `batch_id`, `source_system`, `record_hash`, `load_status`), `source_line_number`,
`raw_payload` (original line) and `_rescued_data` (schema drift).

| Silver table | Source | Key | Transformations |
|---|---|---|---|
| `silver_suppliers` | `bronze_suppliers` (latest batch) | supplier_id | trim/upper codes; INT, BOOLEAN, DATE casts; invalid ISO currency → NULL; duplicates quarantined |
| `silver_products` | `bronze_products` (latest batch) | product_id | DECIMAL casts; negative quantities → NULL; currency case fixed; unknown alternate supplier → NULL |
| `silver_purchase_orders` | `bronze_purchase_orders` (all batches) | po_id, po_line_number | casts; latest `last_modified_ts` wins; FK supplier/product; `rate_to_eur` valid on `order_date` |
| `silver_shipment_events` | `bronze_shipment_events` | event_id, po_line_number | timestamps → UTC; `location.*` → `location_*`; `items[]` exploded (one row per event × PO line); dedup; lifecycle; FKs |
| `silver_fx_rates` | seed in `src/silver/reference.py` | currency, valid_from | brief `fx_rates` |
| `silver_quarantine` | all checks | entity, record_key, rule | blocked records with rule, reason, raw payload, processing timestamp |
| `ops_dq_issues` | all checks | – | every rule hit (QUARANTINE, NULLIFY, FIX, FLAG) |

| Gold table | Source | Logic |
|---|---|---|
| `gold_fact_purchase_order_line` | silver_purchase_orders (non-cancelled) + silver_shipment_events + silver_suppliers | brief derived fields received_qty, is_delivered, actual_delivery_date, latest_eta_date, delay_days, is_on_time, is_delayed, delayed_qty |
| `gold_fact_shipment_event` | silver_shipment_events (distinct event) + silver_purchase_orders (supplier) | one row per status event |
| `gold_fact_inventory_exposure` | silver_products + silver_suppliers + PO-line derived fields + silver_fx_rates | days_of_cover, delayed_inbound_qty, days_until_arrival, unit_cost, rate_to_eur |
| `gold_dim_*` | silver masters, event locations, dictionary status / risk lists | surrogate keys xxhash64, Unknown member -1 |

## 5. Sensitive data

Confidential columns of the data dictionary (`unit_cost`, `unit_price`, `avg_daily_demand`,
`contract_lead_time_days`, `risk_rating`) keep the label in their Gold column comment. To be enforced in the
semantic model (object-level security) and in the API/LLM layer.

## 6. Assumptions and open questions

1. **FX rates**: the brief defines the `fx_rates` seed but not the values — placeholders are used.
2. **FX for `unit_cost`** (Exposed Value): the brief converts "at the FX rate valid on `order_date`", but a product has no order date → rate valid on `snapshot_date` assumed.
3. **`dim_risk_level`**: required by the assignment, not defined by the brief → supplier `risk_rating` (LOW, MEDIUM, HIGH) assumed (alternative: product `criticality`).
4. **"Earliest delayed open line"** (`days_until_arrival`) → earliest `promised_delivery_date` assumed.
5. **Lead time when there is no ETA** → `contract_lead_time_days` of the line's supplier assumed.
6. **Cancelled lines** "are excluded" → removed from the Gold PO-line fact (kept in Silver).
7. **Flattened JSON names** are not in the dictionary → `location.location_id` → `location_id`, `location.name` → `location_name`, `location.country` → `location_country`, `location.type` → `location_type`; `items[].*` → `po_line_number`, `product_id`, `quantity`.
8. **Bronze extras**: `raw_payload` (assignment §3 "retain the original source representation"), `source_line_number` (deterministic first-occurrence dedup, traceable quarantine) and `_rescued_data` (schema-drift response, §6.1) — please confirm.
9. **Events with no valid PO line** (23 events whose only lines reference an unknown product) have no Silver row under the dictionary grain; their lines are in `silver_quarantine`.
10. **Dates of events** are compared in UTC (e.g. "on or before the promised date").
11. **Layer prefixes** are added to the dictionary/assignment names (single schema `team4`).
12. **`ordered_qty = 0`** is quarantined because the dictionary states "must be > 0".
