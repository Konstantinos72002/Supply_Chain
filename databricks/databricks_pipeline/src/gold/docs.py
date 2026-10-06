"""
Table and column descriptions for the Gold model (assignment 6.3). Attached as Delta column
comments by the pipeline and rendered into docs/data_model.md. Sensitivity = data_dictionary.md.
"""
from pyspark.sql import DataFrame

# data_dictionary.md sensitivity (columns not listed are Internal); derived amounts inherit Confidential
CONFIDENTIAL = {"contract_lead_time_days", "risk_rating", "unit_cost", "avg_daily_demand", "unit_price"}
PUBLIC = {"country_code", "region", "product_name", "category", "unit_of_measure", "location_name",
          "location_country", "location_type"}

_KEYS = {
    "supplier_key": "Surrogate key -> gold_dim_supplier (-1 = unknown)",
    "product_key": "Surrogate key -> gold_dim_product (-1 = unknown)",
    "location_key": "Surrogate key -> gold_dim_location (-1 = unknown)",
    "risk_level_key": "Supplier risk_rating -> gold_dim_risk_level (-1 = unknown)",
    "shipment_status_key": "Surrogate key -> gold_dim_shipment_status (-1 = unknown)",
}

TABLES = {
    "gold_fact_purchase_order_line": ("Grain: one purchase-order line (current version, cancelled lines excluded). "
                                      "KPIs: Order Value, On-Time Delivery Rate, Average Delay, Supplier Fill Rate, "
                                      "Delayed Quantity", {
        "po_id": "Purchase order identifier", "po_line_number": "Line number within the purchase order",
        "location_key": "Destination warehouse (delivery_location_id) -> gold_dim_location",
        "order_date_key": "order_date -> gold_dim_date",
        "promised_delivery_date_key": "promised_delivery_date -> gold_dim_date",
        "actual_delivery_date_key": "actual_delivery_date -> gold_dim_date (-1 = not delivered)",
        "latest_eta_date_key": "latest_eta_date -> gold_dim_date (-1 = no shipment event)",
        "line_status": "OPEN, PARTIALLY_RECEIVED, CLOSED",
        "ordered_qty": "Quantity ordered", "unit_price": "Agreed price per unit (line currency)",
        "rate_to_eur": "FX rate to EUR valid on order_date (fx_rates)",
        "received_qty": "Brief: SUM(items[].quantity) from DELIVERED events for the PO line",
        "delay_days": "Brief: delivered -> actual - promised; open and past due -> snapshot - promised; else 0",
        "delayed_qty": "Brief: past due -> ordered - qty received by promised date; not yet due with late ETA -> "
                       "ordered - received; else 0",
        "is_delivered": "Brief: received_qty >= ordered_qty OR line_status = 'CLOSED'",
        "is_on_time": "Brief: is_delivered AND actual_delivery_date <= promised_delivery_date",
        "is_delayed": "Brief: (delivered AND delay_days > 0) OR (not delivered AND (past due OR latest ETA after "
                      "promised))",
    }),
    "gold_fact_shipment_event": ("Grain: one shipment status event", {
        "event_id": "Unique event identifier", "shipment_id": "Shipment the event belongs to",
        "po_id": "Purchase order being shipped", "location_key": "Event location -> gold_dim_location",
        "supplier_key": "Supplier of the purchase order -> gold_dim_supplier",
        "event_date_key": "Event date (UTC) -> gold_dim_date", "carrier": "Logistics carrier name",
        "delay_reason_code": "Reason for delay (nullable)", "event_ts": "Time the status occurred (UTC)",
        "eta_ts": "Carrier's estimated arrival time (UTC)",
    }),
    "gold_fact_inventory_exposure": ("Grain: one product at snapshot_date. KPIs: Exposed Value, Critical-Item Count",
                                     {
        "snapshot_date_key": "snapshot_date (as-of date of the refresh) -> gold_dim_date",
        "supplier_key": "Primary supplier -> gold_dim_supplier",
        "location_key": "Stocking warehouse -> gold_dim_location",
        "on_hand_qty": "Stock on hand at extract time", "safety_stock_qty": "Minimum stock to be held as buffer",
        "avg_daily_demand": "Average units consumed per day", "unit_cost": "Standard cost per unit",
        "rate_to_eur": "FX rate to EUR of cost_currency valid on snapshot_date (fx_rates)",
        "days_of_cover": "Brief: MAX(0, on_hand_qty - safety_stock_qty) / avg_daily_demand",
        "delayed_inbound_qty": "Brief: SUM(delayed_qty) of open delayed lines for the product",
        "days_until_arrival": "Brief: latest_eta_date of earliest delayed open line - snapshot_date; "
                              "if no ETA, delay_days + contract_lead_time_days",
    }),
    "gold_dim_date": ("Date dimension; date_key = yyyyMMdd, -1 = unknown", {
        "date_key": "Surrogate key yyyyMMdd", "date": "Calendar date", "year": "Year",
        "quarter": "Quarter 1-4", "month": "Month 1-12", "month_name": "Month name",
    }),
    "gold_dim_supplier": ("Suppliers (current snapshot)", {
        "supplier_id": "Unique supplier identifier", "supplier_name": "Legal or trading name of the supplier",
        "country_code": "ISO 3166-1 alpha-2 country of the supplier", "region": "EMEA, APAC, AMER",
        "default_currency": "ISO 4217 currency used by the supplier",
        "contract_lead_time_days": "Contractually agreed lead time in days", "risk_rating": "LOW, MEDIUM, HIGH",
        "is_active": "Whether the supplier is currently approved for ordering",
        "onboarded_date": "Date the supplier was approved",
    }),
    "gold_dim_product": ("Products (current snapshot)", {
        "product_id": "Unique product identifier", "product_name": "Product description",
        "category": "Product category",
        "unit_of_measure": "EA, KG, L", "criticality": "CRITICAL, HIGH, MEDIUM, LOW",
        "primary_supplier_id": "Main supplier for the product", "alternate_supplier_id": "Approved backup supplier",
        "stocking_location_id": "Primary warehouse holding the product",
        "has_active_alternate": "Brief: alternate_supplier_id not null AND <> primary_supplier_id AND alternate "
                                "supplier is_active",
    }),
    "gold_dim_location": ("Locations of shipment events and warehouses referenced by PO lines / products", {
        "location_id": "Location identifier", "location_name": "Location name",
        "location_country": "ISO 3166-1 alpha-2 country of the location",
        "location_type": "WAREHOUSE, PORT, HUB, SUPPLIER_SITE",
    }),
    "gold_dim_shipment_status": ("Shipment statuses (data dictionary)", {"status": "Shipment status"}),
    "gold_dim_risk_level": ("Supplier risk levels (data dictionary risk_rating)", {"risk_level": "LOW, MEDIUM, HIGH"}),
}


def column_doc(table: str, column: str) -> str | None:
    return TABLES[table][1].get(column) or _KEYS.get(column)


def sensitivity(column: str) -> str:
    return "Confidential" if column in CONFIDENTIAL else "Public" if column in PUBLIC else "Internal"


def document(df: DataFrame, table: str) -> DataFrame:
    for c in df.columns:
        desc = column_doc(table, c)
        if desc:
            df = df.withMetadata(c, {"comment": f"{desc} [{sensitivity(c)}]"})
    return df
