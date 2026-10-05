"""
Unit tests for the Bronze transformations used by the ETL pipeline.
Auto Loader is replaced by a plain batch read of the same files - the
transformation code under test is identical.   Run:  pytest -q tests/
"""
import os
import shutil

from pyspark.sql import functions as F

from bronze.config import SOURCES, csv_schema
from bronze.transforms import AUDIT_COLUMNS, build_bronze, file_summary


def read_csv(spark, path, source):
    return (spark.read.option("header", True).option("multiLine", True)
            .option("escape", '"').schema(csv_schema(source)).csv(path))


def read_jsonl(spark, path):
    return spark.read.text(path)


def test_csv_sources_keep_all_rows_and_raw_values(spark, data_dir):
    expected = {"suppliers": 51, "products": 302, "purchase_orders": 4020}
    for src, n in expected.items():
        df = build_bronze(read_csv(spark, os.path.join(data_dir, f"{src}.csv"), src), src)
        assert df.count() == n
        assert df.columns == SOURCES[src]["expected_columns"] + ["_rescued_data"] + AUDIT_COLUMNS
        assert df.where("load_status <> 'LOADED'").count() == 0
    sup = build_bronze(read_csv(spark, os.path.join(data_dir, "suppliers.csv"), "suppliers"), "suppliers")
    assert sup.where("supplier_id = 'SUP-0007'").first().default_currency == "EURO"   # defect kept
    assert sup.where("supplier_id = 'SUP-0050'").count() == 2                          # duplicate kept
    assert sup.where(F.col("onboarded_date").contains("\r")).count() == 0             # CRLF handled


def test_jsonl_events_parsed_with_nested_structure(spark, data_dir):
    df = build_bronze(read_jsonl(spark, os.path.join(data_dir, "shipment_events.json")),
                      "shipment_events")
    assert df.count() == 9431
    assert df.where("load_status <> 'LOADED'").count() == 0
    row = df.where("event_id = 'EVT-0000002'").first()
    assert row.location.location_id == "SITE-SUP-0040"
    assert [i.product_id for i in row.items] == ["PRD-00080", "PRD-95922"]
    assert row.raw_payload.startswith('{"event_id": "EVT-0000002"')


def test_malformed_and_extra_keys_in_json(spark, tmp_path):
    p = tmp_path / "shipment_events_x.json"
    p.write_text('{"event_id":"E1","status":"CREATED","items":[{"po_line_number":1,'
                 '"product_id":"P","quantity":5.0}],"new_field":1}\n'
                 '{"event_id": "E2", "status": "DELIV\n\n')
    df = build_bronze(read_jsonl(spark, str(p)), "shipment_events")
    status = {r.event_id: r.load_status for r in df.collect()}
    assert df.count() == 2                                       # blank line ignored
    assert status["E1"] == "SCHEMA_DRIFT"
    assert list(status.values()).count("MALFORMED") == 1
    assert df.where("event_id = 'E1'").first().items[0].quantity == "5.0"


def test_errors_answer_keys_are_excluded(spark, data_dir, tmp_path):
    folder = tmp_path / "suppliers"
    folder.mkdir()
    shutil.copy(os.path.join(data_dir, "suppliers.csv"), folder)
    shutil.copy(os.path.join(data_dir, "suppliers_errors.csv"), folder)
    df = build_bronze(read_csv(spark, str(folder), "suppliers"), "suppliers")
    assert df.count() == 51
    assert df.select("source_file_name").distinct().count() == 1


def test_batch_id_is_deterministic_per_file_version(spark, data_dir):
    path = os.path.join(data_dir, "products.csv")
    a = build_bronze(read_csv(spark, path, "products"), "products").select("batch_id").distinct().collect()
    b = build_bronze(read_csv(spark, path, "products"), "products").select("batch_id").distinct().collect()
    assert len(a) == 1 and a == b and a[0].batch_id.startswith("products-")


def test_record_hash_changes_with_content(spark, tmp_path):
    p = tmp_path / "s.csv"
    hdr = ",".join(SOURCES["suppliers"]["expected_columns"])
    p.write_text(f"{hdr}\nSUP-1,A,DE,EMEA,EUR,10,LOW,true,2020-01-01\n"
                 f"SUP-1,A,DE,EMEA,EUR,12,LOW,true,2020-01-01\n"
                 f"SUP-1,A,DE,EMEA,EUR,12,LOW,true,2020-01-01\n")
    df = build_bronze(read_csv(spark, str(p), "suppliers"), "suppliers")
    assert df.select("record_hash").distinct().count() == 2


def test_file_summary_reconciles(spark, data_dir):
    df = build_bronze(read_csv(spark, os.path.join(data_dir, "purchase_orders.csv"),
                               "purchase_orders"), "purchase_orders")
    s = file_summary(df, "purchase_orders").first()
    assert s.rows_written == 4020 and s.rows_loaded == 4020 and s.rows_malformed == 0
