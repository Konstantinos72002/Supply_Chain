"""
End-to-end: the ONE pipeline (3 source files) from raw landing files to Gold.
Run: BRONZE_TEST_DATA=<files> pytest -q tests/test_pipeline_end_to_end.py
"""
import os
import shutil

import pytest

from pipeline_runner import SRC_DIR, run_pipeline


@pytest.fixture(scope="module")
def run(spark, data_dir, tmp_path_factory):
    landing = tmp_path_factory.mktemp("landing")
    for src, ext in [("suppliers", "csv"), ("products", "csv"), ("purchase_orders", "csv"),
                     ("shipment_events", "json")]:
        (landing / src).mkdir()
        shutil.copy(os.path.join(data_dir, f"{src}.{ext}"), landing / src)
    # an answer key dropped in a source folder by mistake must not be ingested
    shutil.copy(os.path.join(data_dir, "suppliers_errors.csv"), landing / "suppliers")
    return run_pipeline(spark, {"pipeline.src_path": os.path.abspath(SRC_DIR),
                                "bronze.landing_root": str(landing),
                                "gold.as_of_date": "2026-10-05",
                                "spark.sql.session.timeZone": "UTC"})


def test_one_pipeline_three_files_all_layers(run):
    assert [os.path.basename(f) for f in run["files"]] == ["01_bronze.py", "02_silver.py", "03_gold.py"]
    names = set(run["definitions"])
    for t in ["bronze_suppliers", "bronze_products", "bronze_purchase_orders",
              "bronze_shipment_events", "ops_bronze_ingestion_log",
              "silver_suppliers", "silver_products", "silver_purchase_orders", "silver_shipment_events",
              "silver_fx_rates", "silver_quarantine", "ops_dq_issues",
              "gold_fact_purchase_order_line", "gold_fact_shipment_event", "gold_fact_inventory_exposure",
              "gold_dim_date", "gold_dim_supplier", "gold_dim_product", "gold_dim_location",
              "gold_dim_shipment_status", "gold_dim_risk_level"]:
        assert t in names, t


def test_row_flow_through_the_layers(run):
    t = run["tables"]
    counts = {n: t[n].count() for n in [
        "bronze_suppliers", "bronze_products", "bronze_purchase_orders", "bronze_shipment_events",
        "silver_suppliers", "silver_products", "silver_purchase_orders", "silver_shipment_events",
        "gold_fact_purchase_order_line", "gold_fact_shipment_event", "gold_fact_inventory_exposure"]}
    assert counts == {
        "bronze_suppliers": 51, "bronze_products": 302, "bronze_purchase_orders": 4020,
        "bronze_shipment_events": 9431,                              # 13,804 raw records, _errors file excluded
        "silver_suppliers": 50, "silver_products": 300, "silver_purchase_orders": 3911,
        "silver_shipment_events": 22933,                             # one row per event x PO line
        "gold_fact_purchase_order_line": 3798,                       # cancelled lines excluded (brief)
        "gold_fact_shipment_event": 9294, "gold_fact_inventory_exposure": 300}
    log = {r.target_table: (r.input_count, r.written_count, r.rejected_count)
           for r in t["ops_bronze_ingestion_log"].collect()}
    assert log["bronze_purchase_orders"] == (4020, 4020, 0) and len(log) == 4


def test_bronze_expectations_clean_and_gold_documented(run):
    for name in ["bronze_suppliers", "bronze_products", "bronze_purchase_orders", "bronze_shipment_events"]:
        assert all(v == 0 for v in run["expectations"][name].values()), (name, run["expectations"][name])
    gold = [n for n in run["tables"] if n.startswith("gold_")]
    for n in gold:
        assert all("comment" in f.metadata for f in run["tables"][n].schema.fields), n
    assert run["definitions"]["gold_fact_purchase_order_line"]["cluster_by"] == ["order_date_key", "supplier_key"]


def test_brief_kpis_end_to_end(run):
    from test_gold import gold_kpis
    k = gold_kpis(run["tables"])
    assert round(float(k["Order Value"]), 2) == 190388429.60
    assert round(k["On-Time Delivery Rate"], 4) == 0.6446 and round(float(k["Supplier Fill Rate"]), 4) == 0.8426
    assert k["Critical-Item Count"] == 22 and round(k["Alternate-Supplier Coverage"], 4) == 0.5862
