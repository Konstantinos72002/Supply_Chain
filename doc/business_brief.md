# Business Brief – Team 4: Supply Chain Disruption Analytics

## 1. Problem Statement

The company cannot reliably see which purchase orders and shipments are delayed, which suppliers are underperforming, and which products will run out of stock before delayed goods arrive. Supplier, product, purchase-order and shipment-event data are delivered as separate CSV and JSON extracts with inconsistent currencies, unvalidated statuses, duplicate order lines and unknown supplier or product references. Analysts reconcile them manually, so delay and exposure figures are late, inconsistent and not traceable. Mitigation actions such as expediting orders or switching to an alternate supplier are taken reactively, without a shared, trusted view of value at risk.

## 2. Users

| User | Need |
|---|---|
| Supply Chain Director | Overall delivery performance, value at risk and trends |
| Procurement Manager | Supplier reliability and order fulfilment |
| Supply Chain Planner | Delayed lines, expected arrivals and stock-out risk by product and location |
| Supplier Relationship Manager | Supplier scorecards to support reviews and negotiations |
| Warehouse / Inventory Manager | Inbound delays and inventory cover per location |

## 3. Decisions

| Decision | Primary User | Supporting KPIs |
|---|---|---|
| Which delayed orders to expedite first | Supply Chain Planner | Exposed Value, Delayed Quantity, Critical-Item Count |
| When to switch a product to its alternate supplier | Procurement Manager | Exposed Value, Alternate-Supplier Coverage |
| Which suppliers to escalate, review or replace | Supplier Relationship Manager | On-Time Delivery Rate, Supplier Fill Rate, Average Delay |
| Which critical products need a backup supplier | Procurement Manager | Alternate-Supplier Coverage, Critical-Item Count |
| Where to raise safety stock | Warehouse / Inventory Manager | Exposed Value, Average Delay |
| Whether overall supply performance is improving | Supply Chain Director | Order Value, On-Time Delivery Rate, Supplier Fill Rate |

## 4. KPIs

All monetary values are in EUR, converted at the FX rate valid on `order_date`. Cancelled lines are excluded. `snapshot_date` is the as-of date of the refresh.

| KPI | Definition | Formula |
|---|---|---|
| Order Value | Total value of purchase-order lines | Σ (ordered_qty × unit_price × rate_to_eur) |
| On-Time Delivery Rate | Share of delivered lines received on or before the promised date | count(lines where is_on_time) / count(lines where is_delivered) |
| Average Delay (days) | Average lateness of delivered lines that arrived late | AVG(actual_delivery_date − promised_delivery_date) where is_delivered AND delay_days > 0 |
| Supplier Fill Rate | Share of ordered quantity received for lines past their promised date | Σ MIN(received_qty, ordered_qty) / Σ ordered_qty where promised_delivery_date ≤ snapshot_date |
| Exposed Value | Value of delayed inbound goods for products that will run out of usable stock before arrival | Σ (delayed_inbound_qty × unit_cost × rate_to_eur) where days_of_cover < days_until_arrival, at latest snapshot |
| Delayed Quantity | Units not received by the promised date, plus open units with an ETA after the promised date | Σ delayed_qty |
| Critical-Item Count | Number of CRITICAL products with at least one delayed open inbound line | DISTINCTCOUNT(product_id) where criticality = 'CRITICAL' AND delayed_inbound_qty > 0, at latest snapshot |
| Alternate-Supplier Coverage | Share of CRITICAL products with an active alternate supplier different from the primary | count(CRITICAL products where has_active_alternate) / count(CRITICAL products) |

| Derived Field | Formula |
|---|---|
| received_qty | Σ items[].quantity from DELIVERED events for the PO line |
| is_delivered | received_qty ≥ ordered_qty OR line_status = 'CLOSED' |
| actual_delivery_date | Date of the last DELIVERED event for the PO line |
| delay_days | Delivered: actual_delivery_date − promised_delivery_date; open and past due: snapshot_date − promised_delivery_date; otherwise 0 |
| is_on_time | is_delivered AND actual_delivery_date ≤ promised_delivery_date |
| is_delayed | (is_delivered AND delay_days > 0) OR (NOT is_delivered AND (promised_delivery_date ≤ snapshot_date OR latest_eta_date > promised_delivery_date)) |
| delayed_qty | Past due: ordered_qty − qty received by promised_delivery_date; not yet due with late ETA: ordered_qty − received_qty; otherwise 0 |
| delayed_inbound_qty | Σ delayed_qty of open delayed lines for the product |
| days_of_cover | MAX(0, on_hand_qty − safety_stock_qty) / avg_daily_demand |
| days_until_arrival | latest_eta_date of earliest delayed open line − snapshot_date; if no ETA, delay_days + contract_lead_time_days |
| has_active_alternate | alternate_supplier_id is not null AND ≠ primary_supplier_id AND alternate supplier is_active |

## 5. Dataset Inventory

| Dataset | Format | Data Class | Grain | Primary Key | Approx. Records | Delivery |
|---|---|---|---|---|---|---|
| suppliers.csv | CSV | Reference | One supplier | supplier_id | 50 | Full extract per batch |
| products.csv | CSV | Reference | One product | product_id | 300 | Full extract per batch |
| purchase_orders.csv | CSV | Transactional | One purchase-order line | (po_id, po_line_number) | 4,000 | Incremental by last_modified_ts |
| shipment_events.json | JSON (nested) | Semi-structured event stream | One shipment status event | event_id | 8,000 | Incremental by event_ts |
| fx_rates | Configuration seed | Reference | One currency per validity date | (currency, valid_from) | 10 | Versioned with code |

All Bronze tables add audit columns: `source_file_name`, `ingestion_ts`, `batch_id`, `source_system`, `record_hash` and `load_status`.

## 6. Data Dictionary

| Sensitivity | Meaning |
|---|---|
| Public | Non-sensitive, shareable externally |
| Internal | Internal business use only |
| Confidential | Commercially sensitive; restricted access |

### suppliers.csv

Grain: one row per supplier. Primary key: `supplier_id`.

| Field | Type | Key | Business Meaning | Sensitivity |
|---|---|---|---|---|
| supplier_id | string | PK | Unique supplier identifier | Internal |
| supplier_name | string | | Legal or trading name of the supplier | Internal |
| country_code | string | | ISO 3166-1 alpha-2 country of the supplier | Public |
| region | string | | Supplier region (EMEA, APAC, AMER) | Public |
| default_currency | string | | ISO 4217 currency used by the supplier | Internal |
| contract_lead_time_days | integer | | Contractually agreed lead time in days | Confidential |
| risk_rating | string | | Supplier risk level (LOW, MEDIUM, HIGH) | Confidential |
| is_active | boolean | | Whether the supplier is currently approved for ordering | Internal |
| onboarded_date | date | | Date the supplier was approved | Internal |

### products.csv

Grain: one row per product. Primary key: `product_id`.

| Field | Type | Key | Business Meaning | Sensitivity |
|---|---|---|---|---|
| product_id | string | PK | Unique product identifier | Internal |
| product_name | string | | Product description | Public |
| category | string | | Product category | Public |
| unit_of_measure | string | | Unit of quantity (EA, KG, L) | Public |
| unit_cost | decimal(18,4) | | Standard cost per unit | Confidential |
| cost_currency | string | | ISO 4217 currency of `unit_cost` | Internal |
| criticality | string | | Business criticality (CRITICAL, HIGH, MEDIUM, LOW) | Internal |
| primary_supplier_id | string | FK → suppliers.supplier_id | Main supplier for the product | Internal |
| alternate_supplier_id | string | FK → suppliers.supplier_id (nullable) | Approved backup supplier | Internal |
| stocking_location_id | string | | Primary warehouse holding the product | Internal |
| on_hand_qty | decimal(18,2) | | Stock on hand at extract time | Internal |
| safety_stock_qty | decimal(18,2) | | Minimum stock to be held as buffer | Internal |
| avg_daily_demand | decimal(18,2) | | Average units consumed per day | Confidential |

### purchase_orders.csv

Grain: one row per purchase-order line. Primary key: (`po_id`, `po_line_number`).

| Field | Type | Key | Business Meaning | Sensitivity |
|---|---|---|---|---|
| po_id | string | PK (composite) | Purchase order identifier | Internal |
| po_line_number | integer | PK (composite) | Line number within the purchase order | Internal |
| supplier_id | string | FK → suppliers.supplier_id | Supplier fulfilling the line | Internal |
| product_id | string | FK → products.product_id | Product ordered | Internal |
| order_date | date | | Date the order was placed | Internal |
| promised_delivery_date | date | | Supplier-confirmed delivery date | Internal |
| ordered_qty | decimal(18,2) | | Quantity ordered; must be > 0 | Internal |
| unit_price | decimal(18,4) | | Agreed price per unit | Confidential |
| currency | string | | ISO 4217 currency of `unit_price` | Internal |
| delivery_location_id | string | | Destination warehouse for the line | Internal |
| line_status | string | | OPEN, PARTIALLY_RECEIVED, CLOSED, CANCELLED | Internal |
| last_modified_ts | timestamp (UTC) | | Last update time; latest version wins | Internal |

### shipment_events.json

Grain: one object per shipment status event; flattened in Silver to one row per event × PO line. Primary key: `event_id`.

| Field | Type | Key | Business Meaning | Sensitivity |
|---|---|---|---|---|
| event_id | string | PK | Unique event identifier | Internal |
| shipment_id | string | | Shipment the event belongs to | Internal |
| po_id | string | FK → purchase_orders.po_id | Purchase order being shipped | Internal |
| carrier | string | | Logistics carrier name | Internal |
| status | string | | CREATED, PICKED_UP, IN_TRANSIT, CUSTOMS, OUT_FOR_DELIVERY, DELIVERED, DELAYED, CANCELLED | Internal |
| event_ts | timestamp (ISO 8601 with offset) | | Time the status occurred | Internal |
| location.location_id | string | | Location where the event occurred | Internal |
| location.name | string | | Location name | Public |
| location.country | string | | ISO 3166-1 alpha-2 country of the location | Public |
| location.type | string | | WAREHOUSE, PORT, HUB, SUPPLIER_SITE | Public |
| eta_ts | timestamp (ISO 8601 with offset) | | Carrier's estimated arrival time | Internal |
| items[].po_line_number | integer | FK → purchase_orders (po_id, po_line_number) | PO line contained in the shipment | Internal |
| items[].product_id | string | FK → products.product_id | Product shipped | Internal |
| items[].quantity | decimal(18,2) | | Quantity shipped or received for the line | Internal |
| delay_reason_code | string | | Reason for delay (nullable) | Internal |
