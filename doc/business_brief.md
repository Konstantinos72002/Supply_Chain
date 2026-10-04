# Business Brief – Team 4: Supply Chain Disruption Analytics

| | |
|---|---|
| **Version / Date** | 3.5 – for instructor approval (OQ1–OQ4) · 2026-10-04 |
| **Team** | P1 Data Engineering: `[Name Surname]` · P2 Analytics: `[Name Surname]` · P3 AI & API: `[Name Surname]` · P4 Cloud & DevOps: `[Name Surname]` · P5 Project Manager: `[Name Surname]` |
| **Deliverable** | "Business and data brief" (spec §7). The complete field list is in `docs/data_dictionary.md`. |

## Executive Summary

The company buys from about 60 suppliers, but it cannot see in time which shipments are late, which orders and products are affected, which suppliers are unreliable and how much money is at risk. The data exists, but it is spread over separate files, joined by hand in Excel and full of errors.

Our solution supports three decisions: **which suppliers to escalate or replace**, **which orders to expedite**, and **which critical products need an alternate supplier**. The data is loaded and cleaned in Databricks (Bronze → Silver → Gold), shown in a Power BI report on Microsoft Fabric, and made available through a secured API and an AI assistant that use the same numbers.

## 1. Problem Statement

**Scattered data.** Orders, receipts, master data and shipment updates arrive as separate files with no agreed format or schedule, and no record of where each number came from. Analysts join them by hand, so the same figure cannot be reproduced.

**Poor data quality.** The files contain unknown suppliers and products, negative quantities, deliveries dated before the order, mixed currency codes, duplicate order lines and shipments whose status goes backwards. Wrong records are not tracked.

**No shared view.** There is no common definition of "on time" or "at risk", no drill-down to a single late line and no ranked list of critical products that need action.

**Expected value**

| Area | Today | With the solution |
|---|---|---|
| Supplier reporting | ~2 days of manual Excel work per month | Automatic refresh in minutes |
| Delay detection | Monthly review | Late and at-risk lines visible every day |
| Trust in numbers | Different figures per analyst | One definition per KPI; errors detected |
| Action on risk | No priority list | Ranked Mitigation Queue |

*The "Today" values are illustrative assumptions; all data is synthetic.*

## 2. Users and Decisions

| User | Decision | How often | Main KPIs | Page |
|---|---|---|---|---|
| **Executive (COO)** | Overall risk picture; where to intervene | Weekly | Order Value, Exposed Value, On-Time Rate | 1 |
| **Procurement Manager** | Which supplier to escalate or replace | Monthly | On-Time Rate, Fill Rate, Average Delay | 3 |
| **Supply Chain Planner** ¹ | Which orders to expedite | Daily | Exposed Value, Delayed Quantity, Critical-Item Count | 2, 4 |
| **Sourcing Lead** | Which critical products need an alternate supplier | Monthly | Alternate-Supplier Coverage, Critical-Item Count | 5 |
| **Data Steward** | Which rejected records to fix at the source | After each load | Open Quarantine Records | 6 |

¹ Regional Planners have the same role but see only their own region.

**Report pages:** 1 Supply Overview · 2 Delay & Exposure · 3 Supplier Scorecard · 4 Product/Location drill-through · 5 Mitigation Queue · 6 Data Quality (extra; keeps rejected records visible and actionable, spec §6.2). Every page shows **"Data as of"** and **"Last refresh"**.

## 3. Business Questions

| # | Question | KPI(s) | Page | Answered correctly when… |
|---|---|---|---|---|
| Q1 | What is the order value, and how much open value is exposed to delays? | Order Value, Exposed Value | 1 | Values match the Gold tables |
| Q2 | How is the on-time rate trending, and in which regions is it getting worse? | On-Time Rate, monthly change | 1 | Regions with a monthly drop **and** a rate below 95% are highlighted |
| Q3 | Which 5 suppliers had the worst on-time delivery in the last full quarter? | On-Time Rate, Average Delay | 3 | Only suppliers with at least 5 closed lines; same top 5 in every channel |
| Q4 | Which suppliers keep delivering less than ordered? | Fill Rate | 3 | Below 95% in at least 3 of the last 4 full months (see 4.1) |
| Q5 | How much value is exposed by risk level and region, and how much of it is critical? | Exposed Value, Critical-Item Count | 2 | Total equals Q1 for the same region |
| Q6 | What are the main delay reasons for open lines, and how much quantity is behind each? | Delayed Qty – Open | 2 | Reasons add up to the total |
| Q7 | For a product or location: how does stock compare to safety stock, and which lines are late? | Stock Gap, Delayed Qty – Open | 4 | Both are shown; line details show quantities and dates only, no prices |
| Q8 | Which critical exposed products have **no** valid alternate supplier? | Alt.-Supplier Coverage, Critical-Item Count | 5 | Ranked by risk level, then by exposed value |

**Channels.** The report answers all eight questions. The API answers a subset (spec §6.7): the analytics query endpoint answers Q1, Q2, Q5 and Q6, and the **supplier-scorecard endpoint, our domain-specific endpoint** (spec §6.7), answers Q3 and Q4. The AI assistant answers Q1–Q8 through two approved, read-only tools: one returns KPIs and one returns filtered detail lists (spec §6.5). Every assistant answer gives **answer, evidence, filters used, KPI/source and limitations**, and says so when a question is out of scope or the data is missing. For the same user role, all channels return the same numbers.

## 4. KPI Definitions

These definitions are used in the report, the API and the assistant. Targets are stored in one place and never typed into individual charts.

### 4.1 Key terms

- **As-of date:** the latest order, receipt, closing or shipment date in the valid data. All time rules ("late", "next 7 days", "last full quarter") use it instead of today's date, so results do not depend on when the demo runs. It is about 31 July 2026 after the first batch and 30 September 2026 after the second.
- **Full month / quarter:** a period that has ended on or before the as-of date.
- **Open / closed / cancelled line:** *open* while goods are still expected; *closed* when delivery is finished, even if less was delivered. Cancelled lines are left out of all KPIs.
- **Delivery date:** date of the last goods receipt of a line. **Closing date:** date the line was closed.
- **Shipments:** each shipment carries exactly one order line; a line can be split over one or two shipments (partial delivery). Steps are `CREATED → PICKED_UP → IN_TRANSIT → AT_PORT → OUT_FOR_DELIVERY → DELIVERED`; steps may be skipped but never go backwards. A `DELAYED` update marks a shipment as stuck and gives a reason.
- **Delayed line:** delivered after the promised date, **or** still open after the promised date, **or** one of its shipments is currently delayed.
- **At-risk line:** open, not delayed, promised within the next 7 days, and not yet in transit (including lines with no shipment yet).
- **Delay reason:** the reason in the line's latest delay update, or **"Not reported"**.
- **Risk level of a line:** `HIGH` = delayed and either more than 14 days late or a critical product without an alternate supplier · `MEDIUM` = any other delayed line · `LOW` = at risk · `NONE` = all other lines. A product at a location gets the highest level among its open lines.
- **Under-delivering supplier (Q4):** Fill Rate below 95% in at least 3 of the last 4 full months; a month counts only with at least 3 closed lines.
- **Valid alternate supplier:** a supplier other than the primary one, active on the as-of date.
- **Region:** the region of the delivery location. **Units:** values in EUR (one fixed rate per currency, OQ3); quantities in the product's base unit; times in UTC.

### 4.2 The eight KPIs

| KPI | How it is calculated | Counted by | Target (green / amber / red) |
|---|---|---|---|
| **Order Value** (€) | Ordered quantity × unit price | Order date | – |
| **On-Time Delivery Rate** (%) | Closed lines delivered on or before the promised date ÷ closed lines with at least one delivery | Delivery date | ≥ 95 / 90–<95 / < 90 |
| **Average Delay** (days) | Average days late of closed late lines | Delivery date | ≤ 2 / >2–5 / > 5 |
| **Supplier Fill Rate** (%) | Received quantity (max. the ordered quantity) ÷ ordered quantity, closed lines | Closing date | ≥ 95 / 90–<95 / < 90 |
| **Exposed Value** (€) | Still-missing quantity × unit price of delayed or at-risk open lines | As-of date | – |
| **Delayed Quantity** (units) | Two measures: **Open** = missing quantity on delayed open lines; **Late Delivered** = quantity that arrived late | Open: as-of date · Late: delivery date | – |
| **Critical-Item Count** | Critical products with Exposed Value > 0 | As-of date | 0 / 1–5 / ≥ 6 |
| **Alternate-Supplier Coverage** (%) | Critical products with a valid alternate supplier ÷ all critical products | As-of date | 100 / 80–<100 / < 80 |

The two Delayed Quantity measures are counted on different dates, so they are never added together. **Extra measures:** On-Time Rate monthly change, Open Order Value, Exposed Share, Stock Gap (stock minus safety stock) and Open Quarantine Records.

This covers the eight-plus DAX measures of spec §6.4 and all four required types: **volume** (Order Value, Delayed Quantity), **rate** (On-Time Rate, Fill Rate, Coverage), **trend** (On-Time Rate monthly change) and **exception** (Critical-Item Count, Open Quarantine Records).

## 5. Dataset Inventory

All data is **synthetic**, created with a Python script (fixed seed, so it can be recreated exactly). It contains no real company or personal data: only invented company names, no person names, e-mails or phone numbers. It covers orders from January 2025 to September 2026, about 380 order lines per month, 20 locations in 4 regions.

### 5.1 Files and Gold model

| File | Type | Records | How it is loaded |
|---|---|---|---|
| `purchase_orders.csv` | Transactional CSV | ~8,000 lines | New and changed records only |
| `goods_receipts.csv` ¹ | Transactional CSV | ~9,000 (1–2 per line) | New and changed records only |
| `suppliers.csv` | Reference CSV | ~60 | Changes kept as history |
| `products.csv` | Reference CSV | ~300 | Latest details kept; stock kept per snapshot date |
| `locations.csv` ¹ | Reference CSV | ~20 | Latest details kept |
| `shipment_events.json` | Semi-structured JSON | ~9,000 shipments, ~35,000 events (about 4 per shipment) | New records only |

¹ Added to the assigned file list (OQ1, OQ4). The spec asks for at least two related transactional files, and the assigned list has one. Receipts give the received quantity and delivery date; locations give the region needed for regional access.

**Total:** ~52,000 transactional and event records (minimum required: 10,000).

**Gold tables** (what one row means):

| Table | One row is… |
|---|---|
| `fact_purchase_order_line` | one order line |
| `fact_shipment_event` | one shipment status update |
| `fact_inventory_exposure` | one product at one location on one as-of date |

**Dimensions:** `dim_date`, `dim_supplier`, `dim_product`, `dim_location`, `dim_shipment_status`, `dim_risk_level`. Supplier changes are kept as history, so an old order shows the supplier details valid at that time. Each dimension has an "Unknown" row only for values not known yet (e.g. the delivery date of an open line); records with a missing supplier, product, location or order line are quarantined, never hidden under "Unknown". Keys and the star-schema diagram belong to the Dimensional model deliverable.

### 5.2 Refresh strategy

The data arrives in two batches. `batch_001` (orders up to July 2026, ~90% of the data) is the first load; `batch_002` (new orders, corrections and late shipment updates) is loaded live in the demo. Each batch's `products.csv` carries the weekly stock snapshot taken on or just before that batch's as-of date.

- **Reruns** create no duplicates.
- **Corrected records** replace the older version: the newest `updated_at` wins; if two versions have the same time, the one from the newer file wins.
- **Late shipment updates** are placed in the right order by their event time.
- **Rejected records** whose supplier, product, location or order line was missing are retried on every run; their quarantine status changes from `OPEN` to `RESOLVED` once loaded.

Details: `docs/refresh_strategy.md`.

### 5.3 Data-quality rules

DQ01–DQ06 are the rules assigned to Team 4; DQ07–DQ11 add the completeness, validity, referential-integrity and unit checks of spec §6.2. Rejected records go to a quarantine table (rule, reason, batch, time, status `OPEN` / `RESOLVED`) and stay visible on page 6 until fixed. The data script lists every injected error in `injected_defects_manifest.csv`.

| Rule | Check | Action | Injected |
|---|---|---|---|
| DQ01 | Unknown supplier, product or location | Quarantine, retry next run | ~40 |
| DQ02 | Negative or zero quantity | Quarantine | ~30 |
| DQ03 | Shipment status out of order | Quarantine the event | ~50 |
| DQ04 | Delivery dated before the order | Quarantine | ~30 |
| DQ05 | Currency written differently ("eur", "€") or unknown | Correct the spelling; if unknown, quarantine | ~60 |
| DQ06 | Duplicate order lines, receipts or events | Keep the newest version (same time: newer file wins), log the copies | ~80 |
| DQ07 | Required field missing | Quarantine | ~30 |
| DQ08 | Receipt for an order line that does not exist | Quarantine, retry next run | ~20 |
| DQ09 | Line closed with no receipt | Warning; counts as 0 received in Fill Rate | ~25 |
| DQ10 | Unit of measure written differently ("pcs", "PCS"), unknown, or not the product's base unit | Correct the spelling; otherwise quarantine | ~40 |
| DQ11 | Order, receipt, closing or event date after the file was extracted (promised dates excluded) | Quarantine | ~15 |

Test results and quarantine examples belong to the Quality evidence deliverable (`docs/data_quality_rules.md`).

### 5.4 Data dictionary

Sensitivity: **Public** (no restriction), **Internal** (company staff), **Confidential** (unit prices, shown only inside totals). Keys: **PK** = unique identifier, **FK** = reference to another file. All fields are Internal unless marked otherwise.

| File | Field (type) | Key | Business meaning |
|---|---|---|---|
| **purchase_orders.csv** | `po_id` (string), `po_line_number` (integer) | PK (together) | Order number and line number |
| | `supplier_id`, `product_id`, `location_id` (string) | FK → suppliers / products / locations | Supplier, product and delivery location of the line |
| | `order_date`, `promised_date` (date) | – | Date ordered; delivery date promised by the supplier |
| | `ordered_qty` (decimal), `unit_of_measure`, `currency` (string) | – | Quantity ordered and its unit; currency of the price |
| | `unit_price` (decimal) — **Confidential** | – | Price per unit |
| | `line_status` (string), `closed_at`, `updated_at` (timestamp) | – | `OPEN` / `CLOSED` / `CANCELLED`; when closed; last change |
| **goods_receipts.csv** | `receipt_id` (string) | PK | Goods receipt number |
| | `po_id`, `po_line_number` · `shipment_id` (string) | FK → purchase_orders · shipment_events | Order line received; shipment that brought the goods |
| | `receipt_date` (date), `received_qty` (decimal), `unit_of_measure` (string), `updated_at` (timestamp) | – | Date and quantity received, its unit; last change |
| **suppliers.csv** | `supplier_id` (string) | PK | Supplier code |
| | `supplier_name`, `country` (string), `is_active` (boolean), `updated_at` (timestamp) | – | Invented name, country; usable or not; last change (keeps history) |
| **products.csv** | `product_id` (string) | PK | Product code |
| | `product_name`, `category` (string), `is_critical` (boolean) | – | Description, group; critical = a shortage would stop operations |
| | `primary_supplier_id`, `alternate_supplier_id` · `location_id` (string) | FK → suppliers · locations | Main and backup supplier (backup empty if none); stocking location |
| | `base_unit_of_measure` (string), `stock_qty`, `safety_stock_qty` (decimal), `snapshot_date` (date) | – | Unit for all quantities; stock on hand, minimum level, snapshot date |
| **locations.csv** — **Public** | `location_id` (string) | PK | Location code |
| | `location_name`, `country`, `region` (string) | – | Description; region used for regional access |
| **shipment_events.json** (one record per shipment) | `shipment_id` (string) | PK | Shipment number |
| | `po_id`, `po_line_number` (string, integer) | FK → purchase_orders | Order line carried by the shipment |
| | `events[]` (nested list) | – | The shipment's status updates, each with the fields below |
| | `events[].event_id` (string) | PK | Status update number |
| | `events[].event_ts` (timestamp) | – | Time of the update |
| | `events[].status`, `events[].delay_reason` (string) | – | Shipment step or `DELAYED`; reason (only for `DELAYED`) |

**Allowed values.** Regions: North, South, East, West · Currencies: EUR, USD, GBP · Delay reasons: Weather, Customs, Carrier capacity, Supplier production, Port congestion · Units: PCS, KG, L, M.

**Time zones.** `closed_at`, `updated_at` and `event_ts` arrive with mixed offsets (e.g. `+03:00`, `Z`) and are converted to UTC in Silver (spec §6.2).

**Tracking columns.** Every Bronze table also gets load time, source-file extraction time, batch ID, source file name, source system, source key, a record fingerprint (to detect changed records) and load status, so any number can be traced back to its source file and batch (spec §4, §6.1). The source-to-Silver-to-Gold mapping is in `docs/source_to_target_mapping.md`.

**Security.** Single order lines are never shown with prices; prices appear only inside € totals. Regional users see only their own region in the report, the API and the assistant.

## 6. Success Measures

| # | Measure | Target | Evidence |
|---|---|---|---|
| S1 | Report answers all business questions | 8 of 8 "answered correctly" conditions met | Usability test |
| S2 | Report matches the Gold tables | No difference on 6 key values | Validation file |
| S3 | Same numbers in all channels | Report = API = assistant for 3 KPIs, same user role | Screenshots |
| S4 | Injected errors are caught | 100% for every rule | Manifest vs. quarantine table |
| S5 | No records lost | Every loaded record ends up in Silver, in quarantine or in the duplicate log; Silver and Gold counts and € totals match | Reconciliation table |
| S6 | Safe reruns | Same counts after loading `batch_001` again | Load log |
| S7 | Live refresh works | `batch_002` updates the report, API and assistant | Live demo |
| S8 | Assistant does not invent answers | 6 tests pass: one per tool, plus wrong input, missing data, tool failure and out-of-scope question | Test log |
| S9 | Security works | Own region only; no prices per line | Screenshots |

## 7. Scope, Assumptions and Open Questions

**Out of scope:** predicting delays with machine learning, automatic actions on orders, live exchange rates and connections to real company systems. Optional extras, such as the Knowledge Graph bonus, start only after the full mandatory scope works end to end (spec §6, §9).

**Assumptions:** one fixed EUR exchange rate per currency · each product is stocked at one location, and its order lines are delivered there · one Regional Planner sees one region · in the API and the assistant, each user's region is set by their access key · stock comes from a weekly snapshot, so it can be up to 7 days old (known limitation) · the "Today" values in section 1 are illustrative.

**Open questions.** Each question has a fallback, so the rest of this brief stays valid whatever the answer. Questions about the Azure setup and the AI model are tracked in the risk log.

| # | Question for the instructor | Our proposal | Fallback | Needed by |
|---|---|---|---|---|
| OQ1 | May we add `goods_receipts.csv`? | Yes | Received quantity and delivery date as columns on the order lines | Mon 5 Oct |
| OQ2 | How does Fabric read the Gold tables? | Direct access to the Databricks catalog | Copy the Gold tables into Fabric | Mon 5 Oct |
| OQ3 | Is a fixed exchange-rate table acceptable? | Yes, as a known limitation | – | Mon 5 Oct |
| OQ4 | May we add `locations.csv`? | Yes | Location and region as columns in `purchase_orders.csv` | Mon 5 Oct |

Risks (data, access, quality, integration, schedule) are tracked in `docs/risk_log.md`.
