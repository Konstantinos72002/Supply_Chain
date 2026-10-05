"""
Source contracts for the Bronze layer (Lakeflow / DLT ETL pipeline).

Each entry is the *ingestion contract* for one source: landing folder, file
format, expected columns, key column(s) and target Bronze table.

All layers live in ONE schema (data2026acc123456.team4), so the layer is
encoded as a table-name prefix: bronze_*, silver_*, gold_*, ops_*.

Business rules (types, currencies, duplicates, FK checks) are NOT applied
here - they belong to Silver.
"""
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

DEFAULT_CATALOG = "data2026acc123456"
DEFAULT_SCHEMA = "team4"
DEFAULT_LANDING_ROOT = f"/Volumes/{DEFAULT_CATALOG}/{DEFAULT_SCHEMA}/landing"

# Every raw value is kept as STRING in Bronze so defects such as 'usd',
# '-278.00' or '2025-13-40' survive untouched and can be quarantined in Silver.
S = StringType()

SHIPMENT_EVENT_SCHEMA = StructType([
    StructField("event_id", S),
    StructField("shipment_id", S),
    StructField("po_id", S),
    StructField("carrier", S),
    StructField("status", S),
    StructField("event_ts", S),            # kept with its original UTC offset
    StructField("location", StructType([
        StructField("location_id", S),
        StructField("name", S),
        StructField("country", S),
        StructField("type", S),
    ])),
    StructField("eta_ts", S),
    StructField("items", ArrayType(StructType([
        StructField("po_line_number", S),
        StructField("product_id", S),
        StructField("quantity", S),
    ]))),
    StructField("delay_reason_code", S),
])

SOURCES = {
    "suppliers": {
        "format": "csv",
        "folder": "suppliers",
        "target_table": "bronze_suppliers_raw",
        "source_system": "ERP_MASTER_DATA",
        "load_type": "full_snapshot",       # each delivery = full extract, appended as a batch
        "key_columns": ["supplier_id"],
        "expected_columns": [
            "supplier_id", "supplier_name", "country_code", "region",
            "default_currency", "contract_lead_time_days", "risk_rating",
            "is_active", "onboarded_date",
        ],
        "comment": "Raw supplier master data from suppliers*.csv (full snapshot per delivery).",
    },
    "products": {
        "format": "csv",
        "folder": "products",
        "target_table": "bronze_products_raw",
        "source_system": "ERP_MASTER_DATA",
        "load_type": "full_snapshot",
        "key_columns": ["product_id"],
        "expected_columns": [
            "product_id", "product_name", "category", "unit_of_measure",
            "unit_cost", "cost_currency", "criticality", "primary_supplier_id",
            "alternate_supplier_id", "stocking_location_id", "on_hand_qty",
            "safety_stock_qty", "avg_daily_demand",
        ],
        "comment": "Raw product master data from products*.csv (full snapshot per delivery).",
    },
    "purchase_orders": {
        "format": "csv",
        "folder": "purchase_orders",
        "target_table": "bronze_purchase_orders_raw",
        "source_system": "ERP_PROCUREMENT",
        "load_type": "incremental",         # latest last_modified_ts wins (resolved in Silver)
        "key_columns": ["po_id", "po_line_number"],
        "expected_columns": [
            "po_id", "po_line_number", "supplier_id", "product_id", "order_date",
            "promised_delivery_date", "ordered_qty", "unit_price", "currency",
            "delivery_location_id", "line_status", "last_modified_ts",
        ],
        "comment": "Raw purchase-order lines from purchase_orders*.csv (incremental deliveries).",
    },
    "shipment_events": {
        "format": "jsonl",                  # newline-delimited JSON, read line by line
        "folder": "shipment_events",
        "target_table": "bronze_shipment_events_raw",
        "source_system": "CARRIER_TMS",
        "load_type": "incremental",
        "key_columns": ["event_id"],
        "expected_columns": [f.name for f in SHIPMENT_EVENT_SCHEMA.fields],
        "json_schema": SHIPMENT_EVENT_SCHEMA,
        "comment": "Raw carrier shipment events from shipment_events*.json, one row per JSON line, "
                   "nested structure preserved; original line kept in raw_payload.",
    },
}

# DQ answer keys must never be ingested, even if someone drops them in a source folder.
EXCLUDED_FILE_SUFFIXES = ["_errors.csv", "_errors.json"]

# Per-record load_status values written to Bronze
LOAD_STATUS_LOADED = "LOADED"              # parsed and matches the contract
LOAD_STATUS_SCHEMA_DRIFT = "SCHEMA_DRIFT"  # parsed, but carries unexpected / mismatched data
LOAD_STATUS_MALFORMED = "MALFORMED"        # could not be parsed (raw text kept)

# Delta / pipeline properties applied to every Bronze table
BRONZE_TABLE_PROPERTIES = {
    "quality": "bronze",
    "delta.enableChangeDataFeed": "true",   # lets Silver read changes incrementally
}


def csv_schema(source_name: str) -> StructType:
    """All-string schema for a CSV source, in contract order."""
    return StructType([StructField(c, S) for c in SOURCES[source_name]["expected_columns"]])
