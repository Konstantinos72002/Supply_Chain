"""Export the Gold Delta tables used by the API to CSV test fixtures.

The API tests (src/agentic_app/tests/conftest.py) load one CSV per Gold table from
src/agentic_app/tests/data/gold/ into a local Spark session. This script creates those files
from the real Gold tables in Databricks, using the same .env credentials as the API.

Run from the repository root:
    uv run python scripts/export_gold_fixtures.py
"""
import csv
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

TABLES = [
    "gold_fact_purchase_order_line",
    "gold_fact_inventory_exposure",
    "gold_dim_supplier",
    "gold_dim_product",
    "gold_dim_location",
    "gold_dim_date",
]
OUT_DIR = Path(__file__).resolve().parents[1] / "src" / "agentic_app" / "tests" / "data" / "gold"


def to_csv_value(value):
    """Formats a value so that Spark's CSV schema inference reads it back with the right type."""
    if value is None:
        return ""                                   # empty field -> null
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def main() -> None:
    load_dotenv()
    host = os.environ["DATABRICKS_HOST"].strip().removeprefix("https://").rstrip("/")
    warehouse = os.environ["DATABRICKS_WAREHOUSE_ID"].strip()
    catalog = os.environ.get("DATABRICKS_CATALOG", "workspace").strip()
    schema = os.environ.get("DATABRICKS_SCHEMA", "team4_0").strip()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{warehouse}",
                     access_token=os.environ["DATABRICKS_TOKEN"].strip()) as connection:
        with connection.cursor() as cursor:
            for table in TABLES:
                cursor.execute(f"SELECT * FROM {catalog}.{schema}.{table}")
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                path = OUT_DIR / f"{table}.csv"
                with path.open("w", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    writer.writerow(columns)
                    writer.writerows([to_csv_value(v) for v in row] for row in rows)
                print(f"{table}: {len(rows)} rows -> {path.relative_to(OUT_DIR.parents[4])}")


if __name__ == "__main__":
    main()
