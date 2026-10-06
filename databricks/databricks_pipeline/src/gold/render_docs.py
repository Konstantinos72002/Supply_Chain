"""Render docs/data_model.md from the code. Run: BRONZE_TEST_DATA=<files> python tests/render_gold_docs.py"""
from gold.config import DERIVED_FIELDS, KPIS
from gold.docs import TABLES, column_doc, sensitivity

RELATIONSHIPS = [
    ("gold_dim_date", "gold_fact_purchase_order_line", "order / promised / actual delivery / latest ETA date"),
    ("gold_dim_supplier", "gold_fact_purchase_order_line", "supplier"),
    ("gold_dim_product", "gold_fact_purchase_order_line", "product"),
    ("gold_dim_location", "gold_fact_purchase_order_line", "delivery location"),
    ("gold_dim_risk_level", "gold_fact_purchase_order_line", "supplier risk"),
    ("gold_dim_date", "gold_fact_shipment_event", "event date"),
    ("gold_dim_supplier", "gold_fact_shipment_event", "supplier"),
    ("gold_dim_location", "gold_fact_shipment_event", "event location"),
    ("gold_dim_shipment_status", "gold_fact_shipment_event", "status"),
    ("gold_dim_date", "gold_fact_inventory_exposure", "snapshot date"),
    ("gold_dim_product", "gold_fact_inventory_exposure", "product"),
    ("gold_dim_supplier", "gold_fact_inventory_exposure", "primary supplier"),
    ("gold_dim_location", "gold_fact_inventory_exposure", "stocking location"),
    ("gold_dim_risk_level", "gold_fact_inventory_exposure", "supplier risk"),
]


def render(columns: dict) -> str:
    out = ["# Gold data model - Team 4 Supply Chain Disruption Analytics", "",
           "_Generated from `src/gold/` - do not edit by hand._", "", "## Star schema", "", "```mermaid", "erDiagram"]
    out += [f'    {d} ||--o{{ {f} : "{label}"' for d, f, label in RELATIONSHIPS]
    out += ["```", "", "Surrogate keys are `xxhash64(natural key)` (deterministic across refreshes). Every dimension "
            "has an Unknown member with key -1; date keys are yyyyMMdd. Role-playing dates on the PO-line fact: "
            "one active relationship (order date), the others inactive and activated with USERELATIONSHIP.", "",
            "## Grain", "", "| Fact | Grain |", "|---|---|"]
    out += [f"| `{t}` | {TABLES[t][0].split('. ')[0].replace('Grain: ', '')} |" for t in TABLES if "_fact_" in t]
    out += ["", "## KPIs (business_brief.md section 4) and their DAX measures", "",
            "| KPI | Brief formula | DAX measure |", "|---|---|---|"]
    out += [f"| {k} | {b} | `{d}` |" for k, (b, d) in KPIS.items()]
    out += ["", "## Derived fields stored in Gold", "", "| Field | Brief definition |", "|---|---|"]
    out += [f"| `{k}` | {v} |" for k, v in DERIVED_FIELDS.items()]
    out += ["", "## Delta layout", "",
            "`gold_fact_purchase_order_line` is clustered by (`order_date_key`, `supplier_key`) and "
            "`gold_fact_shipment_event` by (`event_date_key`, `supplier_key`): the report filters by period and "
            "supplier, so liquid clustering lets Databricks skip files on those predicates. Dimensions and the "
            "300-row exposure snapshot are read whole and are not clustered.", "", "## Tables and columns", ""]
    for t, (desc, _) in TABLES.items():
        out += [f"### `{t}`", "", desc, "", "| Column | Description | Sensitivity |", "|---|---|---|"]
        out += [f"| `{c}` | {column_doc(t, c)} | {sensitivity(c)} |" for c in columns.get(t, [])]
        out += [""]
    return "\n".join(out)
