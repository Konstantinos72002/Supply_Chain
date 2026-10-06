"""
Gold definitions, aligned 1:1 with business_brief.md section 4 and the Team 4 assignment table.

Gold stores the brief's DERIVED FIELDS (and the inputs of its formulas). The eight KPIs are the
DAX measures of the Fabric semantic model (assignment 6.4); their definitions are kept here so the
model, the documentation and the tests use one source.
"""
UNKNOWN_KEY = -1           # surrogate key of the "Unknown" member in every dimension

RISK_LEVELS = ["LOW", "MEDIUM", "HIGH"]                         # suppliers.risk_rating (data dictionary)
SHIPMENT_STATUSES = ["CREATED", "PICKED_UP", "IN_TRANSIT", "CUSTOMS", "OUT_FOR_DELIVERY",
                     "DELIVERED", "DELAYED", "CANCELLED"]       # shipment_events.status (data dictionary)

# KPI -> (brief formula, DAX measure over the Gold tables)
KPIS = {
    "Order Value": (
        "SUM(ordered_qty x unit_price x rate_to_eur)",
        "SUMX(gold_fact_purchase_order_line, [ordered_qty] * [unit_price] * [rate_to_eur])"),
    "On-Time Delivery Rate": (
        "count(lines where is_on_time) / count(lines where is_delivered)",
        "DIVIDE(CALCULATE(COUNTROWS(gold_fact_purchase_order_line), gold_fact_purchase_order_line[is_on_time]), "
        "CALCULATE(COUNTROWS(gold_fact_purchase_order_line), gold_fact_purchase_order_line[is_delivered]))"),
    "Average Delay (days)": (
        "AVG(actual_delivery_date - promised_delivery_date) where is_delivered AND delay_days > 0",
        "AVERAGEX(FILTER(gold_fact_purchase_order_line, [is_delivered] && [delay_days] > 0), [delay_days])"),
    "Supplier Fill Rate": (
        "SUM(MIN(received_qty, ordered_qty)) / SUM(ordered_qty) where promised_delivery_date <= snapshot_date",
        "VAR s = MAX(gold_fact_inventory_exposure[snapshot_date_key]) "
        "VAR due = FILTER(gold_fact_purchase_order_line, [promised_delivery_date_key] <= s) "
        "RETURN DIVIDE(SUMX(due, MIN([received_qty], [ordered_qty])), SUMX(due, [ordered_qty]))"),
    "Exposed Value": (
        "SUM(delayed_inbound_qty x unit_cost x rate_to_eur) where days_of_cover < days_until_arrival",
        "SUMX(FILTER(gold_fact_inventory_exposure, NOT ISBLANK([days_of_cover]) && NOT ISBLANK([days_until_arrival]) "
        "&& [days_of_cover] < [days_until_arrival]), [delayed_inbound_qty] * [unit_cost] * [rate_to_eur])"),
    "Delayed Quantity": (
        "SUM(delayed_qty)",
        "SUM(gold_fact_purchase_order_line[delayed_qty])"),
    "Critical-Item Count": (
        "DISTINCTCOUNT(product_id) where criticality = 'CRITICAL' AND delayed_inbound_qty > 0",
        "CALCULATE(DISTINCTCOUNT(gold_fact_inventory_exposure[product_key]), "
        "gold_dim_product[criticality] = \"CRITICAL\", gold_fact_inventory_exposure[delayed_inbound_qty] > 0)"),
    "Alternate-Supplier Coverage": (
        "count(CRITICAL products where has_active_alternate) / count(CRITICAL products)",
        "DIVIDE(CALCULATE(COUNTROWS(gold_dim_product), gold_dim_product[criticality] = \"CRITICAL\", "
        "gold_dim_product[has_active_alternate]), "
        "CALCULATE(COUNTROWS(gold_dim_product), gold_dim_product[criticality] = \"CRITICAL\"))"),
}

DERIVED_FIELDS = {
    "received_qty": "SUM(items[].quantity) from DELIVERED events for the PO line",
    "is_delivered": "received_qty >= ordered_qty OR line_status = 'CLOSED'",
    "actual_delivery_date": "Date of the last DELIVERED event for the PO line",
    "delay_days": "Delivered: actual_delivery_date - promised_delivery_date; open and past due: "
                  "snapshot_date - promised_delivery_date; otherwise 0",
    "is_on_time": "is_delivered AND actual_delivery_date <= promised_delivery_date",
    "is_delayed": "(is_delivered AND delay_days > 0) OR (NOT is_delivered AND (promised_delivery_date <= "
                  "snapshot_date OR latest_eta_date > promised_delivery_date))",
    "delayed_qty": "Past due: ordered_qty - qty received by promised_delivery_date; not yet due with late ETA: "
                   "ordered_qty - received_qty; otherwise 0",
    "delayed_inbound_qty": "SUM(delayed_qty) of open delayed lines for the product",
    "days_of_cover": "MAX(0, on_hand_qty - safety_stock_qty) / avg_daily_demand",
    "days_until_arrival": "latest_eta_date of earliest delayed open line - snapshot_date; if no ETA, "
                          "delay_days + contract_lead_time_days",
    "has_active_alternate": "alternate_supplier_id is not null AND <> primary_supplier_id AND alternate "
                            "supplier is_active",
}
