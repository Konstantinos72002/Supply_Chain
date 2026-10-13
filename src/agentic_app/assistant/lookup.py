"""SQL lookup path helpers: deterministic entity resolution and the schema text for the SQL writer.

guard (lookup) -> entity_resolver -> schema_retriever -> sql_writer -> sql_validator -> execute_sql -> result_check
"""
import re
from typing import Any

# Business IDs in the Gold data and the column that holds them, by their first segment. The first segment decides,
# so SITE-SUP-0040 is a location, not supplier SUP-0040.
ID_COLUMNS = {
    'PRD': 'gold_dim_product.product_id',
    'SUP': 'gold_dim_supplier.supplier_id',
    'PO': 'gold_fact_purchase_order_line.po_id',
    'SHP': 'gold_fact_shipment_event.shipment_id',
    'EVT': 'gold_fact_shipment_event.event_id',
    'WH': 'gold_dim_location.location_id',
    'HUB': 'gold_dim_location.location_id',
    'PORT': 'gold_dim_location.location_id',
    'SITE': 'gold_dim_location.location_id',
}
ID_TOKEN = re.compile(r'\b[A-Za-z]{2,4}(?:-[A-Za-z0-9]+)+\b')


def resolve_entities(question: str) -> list[dict[str, str]]:
    """IDs named in the question, normalised to upper case, with the table and column that hold them."""
    found: dict[str, dict[str, str]] = {}
    for token in ID_TOKEN.findall(question):
        value = token.upper()
        column = ID_COLUMNS.get(value.split('-', 1)[0])
        if column and value not in found:
            table, name = column.split('.')
            found[value] = {'id': value, 'table': table, 'column': name}
    return list(found.values())


def ids_in(text: str) -> set[str]:
    """Business IDs mentioned in a text (same patterns as the resolver)."""
    return {t.upper() for t in ID_TOKEN.findall(text) if t.upper().split('-', 1)[0] in ID_COLUMNS}


def schema_text(schema: dict[str, Any]) -> str:
    """Compact, prompt-ready view of GET /analytics/sql/schema."""
    lines = ['Gold tables (write names without catalog/schema):']
    lines += [f"- {t['name']} ({t['grain']}): {', '.join(t['columns'])}" for t in schema['tables']]
    lines.append('Exact code values:')
    lines += [f'- {column}: {", ".join(values)}' for column, values in schema['allowed_values'].items()]
    lines.append('Synonyms:')
    lines += [f'- {words} -> {columns}' for words, columns in schema['synonyms'].items()]
    lines.append('Joins:')
    lines += [f'- {rule}' for rule in schema['joins']]
    lines.append(f"At most {schema['max_rows']} rows are returned.")
    return '\n'.join(lines)
