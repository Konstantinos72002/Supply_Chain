"""
Unit tests for the Bronze transformations used by the ETL pipeline.
Auto Loader (text, wholetext) is replaced by a batch read with the same
options - the transformation code under test is identical.   Run: pytest -q tests/
"""
import os
import shutil

from bronze.config import SOURCES
from bronze.transforms import AUDIT_COLUMNS, build_bronze, ingestion_log


def read_raw(spark, path):
    """Same as the pipeline reader: one row per file, whole text."""
    return spark.read.text(path, wholetext=True)


def bronze(spark, path, source):
    return build_bronze(read_raw(spark, path), source)


def test_csv_sources_keep_all_rows_and_raw_values(spark, data_dir):
    expected = {"suppliers": 51, "products": 302, "purchase_orders": 4020}
    for src, n in expected.items():
        df = bronze(spark, os.path.join(data_dir, f"{src}.csv"), src)
        assert df.count() == n
        assert df.columns == (SOURCES[src]["expected_columns"]
                              + ["_rescued_data", "raw_payload"] + AUDIT_COLUMNS)
        assert df.where("load_status <> 'LOADED'").count() == 0
    sup = bronze(spark, os.path.join(data_dir, "suppliers.csv"), "suppliers")
    assert sup.where("supplier_id = 'SUP-0007'").first().default_currency == "EURO"   # defect kept
    assert sup.where("supplier_id = 'SUP-0050'").count() == 2                          # duplicate kept
    assert sup.where("onboarded_date LIKE '%\r%'").count() == 0                       # CRLF handled
    assert sup.where("supplier_name LIKE '%,%'").count() >= 0                          # quoted commas ok


def test_source_line_numbers_match_the_file(spark, data_dir):
    path = os.path.join(data_dir, "purchase_orders.csv")
    df = bronze(spark, path, "purchase_orders")
    with open(path, newline="") as fh:
        lines = fh.read().split("\r\n")
    for r in df.where("source_line_number IN (2, 489, 613, 4021)").collect():
        assert lines[r.source_line_number - 1] == r.raw_payload
    assert df.where("source_line_number = 613").first().ordered_qty == "2188.10"   # answer key line


def test_jsonl_events_parsed_with_nested_structure(spark, data_dir):
    df = bronze(spark, os.path.join(data_dir, "shipment_events.json"), "shipment_events")
    assert df.count() == 9431
    assert df.where("load_status <> 'LOADED'").count() == 0
    row = df.where("event_id = 'EVT-0000002'").first()
    assert row.source_line_number == 2
    assert row.location.location_id == "SITE-SUP-0040"
    assert [i.product_id for i in row.items] == ["PRD-00080", "PRD-95922"]
    assert row.raw_payload.startswith('{"event_id": "EVT-0000002"')


def test_csv_columns_mapped_by_header_name_with_drift(spark, tmp_path):
    cols = SOURCES["suppliers"]["expected_columns"]
    reordered = list(reversed(cols)) + ["vat_number"]
    vals = dict(zip(cols, ["SUP-1", '"A, Ltd"', "DE", "EMEA", "EUR", "10", "LOW", "true", "2020-01-01"]))
    p = tmp_path / "suppliers_x.csv"
    p.write_text(",".join(reordered) + "\r\n"
                 + ",".join([vals[c] for c in reversed(cols)] + ["DE123"]) + "\r\n"
                 + "SUP-2,too,few\r\n")
    df = bronze(spark, str(p), "suppliers")
    good = df.where("source_line_number = 2").first()
    assert good.supplier_id == "SUP-1" and good.supplier_name == "A, Ltd" and good.region == "EMEA"
    assert good.load_status == "SCHEMA_DRIFT" and "DE123" in good._rescued_data
    bad = df.where("source_line_number = 3").first()
    assert bad.load_status == "MALFORMED" and bad.raw_payload == "SUP-2,too,few"


def test_missing_csv_column_is_drift_not_failure(spark, tmp_path):
    cols = SOURCES["suppliers"]["expected_columns"][:-1]          # onboarded_date missing
    p = tmp_path / "suppliers_y.csv"
    p.write_text(",".join(cols) + "\nSUP-1,A,DE,EMEA,EUR,10,LOW,true\n")
    r = bronze(spark, str(p), "suppliers").first()
    assert r.onboarded_date is None and r.load_status == "SCHEMA_DRIFT"
    assert "onboarded_date" in r._rescued_data


def test_malformed_and_extra_keys_in_json(spark, tmp_path):
    p = tmp_path / "shipment_events_x.json"
    p.write_text('{"event_id":"E1","status":"CREATED","items":[{"po_line_number":1,'
                 '"product_id":"P","quantity":5.0}],"new_field":1}\n'
                 '{"event_id": "E2", "status": "DELIV\n\n')
    df = bronze(spark, str(p), "shipment_events")
    status = {r.source_line_number: r.load_status for r in df.collect()}
    assert status == {1: "SCHEMA_DRIFT", 2: "MALFORMED"}           # blank line ignored
    assert df.where("event_id = 'E1'").first().items[0].quantity == "5.0"


def test_errors_answer_keys_are_excluded(spark, data_dir, tmp_path):
    folder = tmp_path / "suppliers"
    folder.mkdir()
    shutil.copy(os.path.join(data_dir, "suppliers.csv"), folder)
    shutil.copy(os.path.join(data_dir, "suppliers_errors.csv"), folder)
    df = build_bronze(spark.read.option("pathGlobFilter", "*.csv")
                      .text(str(folder), wholetext=True), "suppliers")
    assert df.count() == 51 and df.select("source_file_name").distinct().count() == 1


def test_batch_id_is_deterministic_per_file_version(spark, data_dir):
    path = os.path.join(data_dir, "products.csv")
    a = bronze(spark, path, "products").select("batch_id").distinct().collect()
    b = bronze(spark, path, "products").select("batch_id").distinct().collect()
    assert len(a) == 1 and a == b and a[0].batch_id.startswith("products-")


def test_record_hash_changes_with_content(spark, tmp_path):
    p = tmp_path / "s.csv"
    hdr = ",".join(SOURCES["suppliers"]["expected_columns"])
    p.write_text(f"{hdr}\nSUP-1,A,DE,EMEA,EUR,10,LOW,true,2020-01-01\n"
                 f"SUP-1,A,DE,EMEA,EUR,12,LOW,true,2020-01-01\n"
                 f"SUP-1,A,DE,EMEA,EUR,12,LOW,true,2020-01-01\n")
    df = bronze(spark, str(p), "suppliers")
    assert df.select("record_hash").distinct().count() == 2


def test_ingestion_log_counts(spark, data_dir, tmp_path):
    df = bronze(spark, os.path.join(data_dir, "purchase_orders.csv"), "purchase_orders")
    s = ingestion_log(df, "purchase_orders").first()
    assert (s.input_count, s.written_count, s.rejected_count) == (4020, 4020, 0)
    p = tmp_path / "suppliers_z.csv"
    p.write_text(",".join(SOURCES["suppliers"]["expected_columns"]) + "\nSUP-1,A,DE,EMEA,EUR,10,LOW,true,2020-01-01\n"
                 "SUP-2,broken\n")
    s = ingestion_log(bronze(spark, str(p), "suppliers"), "suppliers").first()
    assert (s.input_count, s.written_count, s.rejected_count) == (2, 1, 1)
