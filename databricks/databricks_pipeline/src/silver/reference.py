"""
Reference data and DQ policy for the Silver layer (versioned with the code).

Everything a reviewer needs to understand *why* a record was kept, fixed,
flagged or quarantined is in this file.
"""
from datetime import date

# --------------------------------------------------------------------------- #
# ISO 4217 codes accepted as transaction currencies.
# 'XXX' ("no currency") is deliberately NOT accepted: an amount without a
# currency cannot be converted to EUR.
# --------------------------------------------------------------------------- #
ISO_CURRENCIES = [
    "EUR", "USD", "GBP", "CHF", "CNY", "INR", "JPY", "KRW", "MXN", "PLN",
    "TWD", "VND", "SEK", "NOK", "DKK", "CZK", "HUF", "RON", "TRY", "CAD",
    "AUD", "BRL", "SGD", "HKD", "THB", "MYR", "IDR", "ZAR", "AED", "SAR",
]

# --------------------------------------------------------------------------- #
# fx_rates seed (business_brief.md section 5): one rate per (currency, valid_from),
# versioned with code. A rate applies from valid_from until the next valid_from.
# !!! PLACEHOLDER VALUES - the brief defines the seed but not the rates (see open questions).
# --------------------------------------------------------------------------- #
FX_RATES = [
    # currency, valid_from, rate_to_eur (EUR per 1 unit of currency)
    ("EUR", date(2000, 1, 1), 1.0),
    ("USD", date(2025, 1, 1), 0.8600),
    ("GBP", date(2025, 1, 1), 1.1500),
    ("CHF", date(2025, 1, 1), 1.0700),
    ("CNY", date(2025, 1, 1), 0.1200),
    ("INR", date(2025, 1, 1), 0.0099),
    ("JPY", date(2025, 1, 1), 0.0058),
    ("KRW", date(2025, 1, 1), 0.00062),
    ("MXN", date(2025, 1, 1), 0.0470),
    ("PLN", date(2025, 1, 1), 0.2350),
]

# --------------------------------------------------------------------------- #
# Value domains (codes are upper-cased before comparison)
# --------------------------------------------------------------------------- #
REGIONS = ["EMEA", "APAC", "AMER"]
RISK_RATINGS = ["LOW", "MEDIUM", "HIGH"]
CRITICALITY = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
UNITS_OF_MEASURE = ["EA", "KG", "L"]
PRODUCT_CATEGORIES = ["Raw Materials", "Packaging", "Electronic Components",
                      "Fasteners", "Mechanical Parts", "Chemicals"]
PO_LINE_STATUSES = ["OPEN", "PARTIALLY_RECEIVED", "CLOSED", "CANCELLED"]
EVENT_STATUSES = ["CREATED", "PICKED_UP", "IN_TRANSIT", "CUSTOMS", "DELAYED",
                  "OUT_FOR_DELIVERY", "DELIVERED", "CANCELLED"]
LOCATION_TYPES = ["SUPPLIER_SITE", "HUB", "PORT", "WAREHOUSE"]
DELAY_REASON_CODES = ["CUSTOMS_HOLD", "WEATHER", "CARRIER_CAPACITY", "VESSEL_DELAY",
                      "PORT_CONGESTION", "SUPPLIER_CAPACITY", "MATERIAL_SHORTAGE",
                      "DOCUMENTATION"]

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"   # ISO 8601 with Z or +hh:mm offset

# --------------------------------------------------------------------------- #
# Dispositions
#   QUARANTINE : row excluded from the Silver table, kept in silver_quarantine
#   NULLIFY    : row kept, the offending value set to NULL
#   FIX        : row kept, value standardised (e.g. 'usd' -> 'USD')
#   FLAG       : row kept unchanged
# Every issue - whatever the disposition - is logged in ops_dq_issues; blocked rows also go to
# silver_quarantine with rule, reason and processing timestamp (assignment 6.2).
# --------------------------------------------------------------------------- #
QUARANTINE, NULLIFY, FIX, FLAG = "QUARANTINE", "NULLIFY", "FIX", "FLAG"

# Rule catalogue (names match the dq_rule values of the *_errors.csv answer keys)
DQ_RULES = {
    "malformed_record":        "Bronze row could not be parsed",
    "schema_drift":            "Unexpected or missing columns/keys in the source file",
    "missing_required":        "Mandatory value is empty",
    "invalid_type":            "Value cannot be converted to the expected data type",
    "invalid_domain":          "Value is not in the allowed list",
    "invalid_value":           "Value violates a data-dictionary rule (e.g. ordered_qty must be > 0)",
    "currency_inconsistency":  "Currency not a valid ISO 4217 code, or differs from supplier default",
    "missing_fx_rate":         "No FX rate valid on order_date for the line currency",
    "negative_quantity":       "Quantity is negative",
    "negative_price":          "Price / cost is negative",
    "duplicate_supplier":      "Same supplier_id more than once in a snapshot",
    "duplicate_product":       "Same product_id more than once in a snapshot",
    "duplicate_order_line":    "Same (po_id, po_line_number) and same last_modified_ts more than once",
    "duplicate_event":         "Same event_id more than once",
    "unknown_supplier":        "supplier_id not found in silver_suppliers",
    "unknown_product":         "product_id not found in silver_products",
    "unknown_purchase_order":  "po_id not found in purchase_orders (data dictionary FK)",
    "unknown_order_line":      "(po_id, items[].po_line_number) not found in purchase_orders (data dictionary FK)",
    "delivery_before_order":   "Delivery date / DELIVERED event before the PO order date",
    "invalid_status_sequence": "Shipment status lifecycle violated",
}
