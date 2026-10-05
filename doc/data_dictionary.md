# Data Dictionary – Supply Chain Disruption Analytics

| Sensitivity | Meaning |
|---|---|
| Public | Non-sensitive, shareable externally |
| Internal | Internal business use only |
| Confidential | Commercially sensitive; restricted access |

## suppliers.csv

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

## products.csv

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

## purchase_orders.csv

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

## shipment_events.json

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
