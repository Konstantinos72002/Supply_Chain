"""
Silver tests on the real pipeline run (generated datasets) + small hand-made scenarios.
Run: BRONZE_TEST_DATA=<files> pytest -q tests/
"""
from pyspark.sql import functions as F

from silver_harness import answer_keys, reconcile, run_on_landing


# ---------------------------------------------------------------- answer keys (injected defects)
def test_every_injected_defect_is_detected(tables, spark, data_dir):
    recall, missed, _ = reconcile(tables["ops_dq_issues"], answer_keys(spark, data_dir))
    rows = recall.collect()
    assert len(rows) == 17 and sum(r.expected for r in rows) == 435
    assert all(r.detected == r.expected for r in rows), [r for r in rows if r.detected != r.expected]
    assert missed.count() == 0


def test_no_line_flagged_outside_the_answer_keys(tables, spark, data_dir):
    _, _, extra = reconcile(tables["ops_dq_issues"], answer_keys(spark, data_dir))
    assert extra.count() == 0


# ---------------------------------------------------------------- shape: data-dictionary names
def test_silver_tables_follow_the_data_dictionary(tables):
    lineage = ["source_file_name", "source_line_number", "batch_id"]
    assert tables["silver_suppliers"].columns == [
        "supplier_id", "supplier_name", "country_code", "region", "default_currency", "contract_lead_time_days",
        "risk_rating", "is_active", "onboarded_date"] + lineage
    assert tables["silver_products"].columns == [
        "product_id", "product_name", "category", "unit_of_measure", "unit_cost", "cost_currency", "criticality",
        "primary_supplier_id", "alternate_supplier_id", "stocking_location_id", "on_hand_qty", "safety_stock_qty",
        "avg_daily_demand"] + lineage
    assert tables["silver_purchase_orders"].columns == [
        "po_id", "po_line_number", "supplier_id", "product_id", "order_date", "promised_delivery_date",
        "ordered_qty", "unit_price", "currency", "rate_to_eur", "delivery_location_id", "line_status",
        "last_modified_ts"] + lineage
    assert tables["silver_shipment_events"].columns == [
        "event_id", "shipment_id", "po_id", "carrier", "status", "event_ts", "location_id", "location_name",
        "location_country", "location_type", "eta_ts", "po_line_number", "product_id", "quantity",
        "delay_reason_code"] + lineage
    assert tables["silver_fx_rates"].columns == ["currency", "valid_from", "rate_to_eur"]
    assert tables["silver_fx_rates"].count() == 10                     # brief: ~10 records
    assert tables["silver_quarantine"].columns == ["entity", "record_key", "rule", "reason", "raw_payload"] + \
        lineage + ["processing_ts"]
    types = dict(tables["silver_purchase_orders"].dtypes)
    assert (types["ordered_qty"], types["unit_price"], types["order_date"], types["last_modified_ts"]) == \
        ("decimal(18,2)", "decimal(18,4)", "date", "timestamp")


# ---------------------------------------------------------------- counts and invariants
def test_row_counts_and_unique_keys(tables):
    expected = {"silver_suppliers": 50, "silver_products": 300, "silver_purchase_orders": 3911,
                "silver_shipment_events": 22933}
    for t, n in expected.items():
        assert tables[t].count() == n, (t, tables[t].count())
    keys = {"silver_suppliers": ["supplier_id"], "silver_products": ["product_id"],
            "silver_purchase_orders": ["po_id", "po_line_number"],
            "silver_shipment_events": ["event_id", "po_line_number"]}
    for t, k in keys.items():
        assert tables[t].count() == tables[t].select(*k).distinct().count(), t
    # 9,317 valid events; 23 of them carry only quarantined lines (unknown product) -> no Silver row
    assert tables["silver_shipment_events"].select("event_id").distinct().count() == 9294


def test_every_bronze_record_is_accounted_for(tables):
    q = tables["silver_quarantine"]
    po_quarantined = q.where("entity = 'purchase_orders'").select("record_key").distinct().count()
    assert tables["silver_purchase_orders"].count() + po_quarantined == tables["bronze_purchase_orders"].count()
    # every Bronze event line ends in Silver (valid lines) or in quarantine (event- or line-level)
    ev_lines = tables["silver_shipment_events"].select("source_line_number") \
        .union(q.where("entity = 'shipment_events'").select("source_line_number")).distinct().count()
    assert ev_lines == tables["bronze_shipment_events"].count()


def test_referential_integrity(tables):
    po, ev = tables["silver_purchase_orders"], tables["silver_shipment_events"]
    assert po.join(tables["silver_suppliers"], "supplier_id", "left_anti").count() == 0
    assert po.join(tables["silver_products"], "product_id", "left_anti").count() == 0
    assert ev.join(tables["silver_products"], "product_id", "left_anti").count() == 0
    assert po.where("ordered_qty <= 0 OR promised_delivery_date < order_date OR rate_to_eur IS NULL").count() == 0


def test_dispositions_on_known_defects(tables):
    sup = {r.supplier_id: r for r in tables["silver_suppliers"].collect()}
    assert sup["SUP-0007"].default_currency is None                       # 'EURO' not ISO 4217 -> NULL
    assert sup["SUP-0050"].source_line_number == 51                       # first occurrence kept
    prd = {r.product_id: r for r in tables["silver_products"].collect()}
    assert prd["PRD-00009"].safety_stock_qty is None                      # negative -> NULL
    assert prd["PRD-00098"].cost_currency == "USD"                        # 'usd' -> 'USD'
    kept = tables["silver_purchase_orders"].where("po_id = 'PO-2025-00196' AND po_line_number = 3").first()
    assert kept.source_line_number == 489
    q = tables["silver_quarantine"].where("entity = 'purchase_orders' AND source_line_number = 613").collect()
    assert [r.rule for r in q] == ["duplicate_order_line"] and q[0].reason and q[0].processing_ts


def test_timestamps_converted_to_utc(tables):
    ev = tables["silver_shipment_events"].where("event_id = 'EVT-0000002'").first()
    assert str(ev.event_ts) == "2025-10-05 08:32:00"                     # 09:32+01:00


# ---------------------------------------------------------------- scenarios (incremental behaviour)
PO_HDR = ("po_id,po_line_number,supplier_id,product_id,order_date,promised_delivery_date,"
          "ordered_qty,unit_price,currency,delivery_location_id,line_status,last_modified_ts\n")
SUP = ("supplier_id,supplier_name,country_code,region,default_currency,contract_lead_time_days,"
       "risk_rating,is_active,onboarded_date\nSUP-0001,A,DE,EMEA,EUR,10,LOW,true,2020-01-01\n")
PRD = ("product_id,product_name,category,unit_of_measure,unit_cost,cost_currency,criticality,"
       "primary_supplier_id,alternate_supplier_id,stocking_location_id,on_hand_qty,safety_stock_qty,"
       "avg_daily_demand\nPRD-00001,Bolt,Fasteners,EA,1.0,EUR,LOW,SUP-0001,,WH-MIL-01,10,5,1\n")
EVT = ('{"event_id":"E%d","shipment_id":"S1","po_id":"PO-1","carrier":"X","status":"%s",'
       '"event_ts":"%s","location":{"location_id":"WH-MIL-01","name":"Milan","country":"IT",'
       '"type":"WAREHOUSE"},"eta_ts":"2025-10-20T10:00:00Z","items":[{"po_line_number":1,'
       '"product_id":"PRD-00001","quantity":5.0}],"delay_reason_code":null}\n')


def _landing(tmp_path, po_files, events):
    import os
    for folder, name, text in [("suppliers", "suppliers.csv", SUP), ("products", "products.csv", PRD),
                               ("shipment_events", "shipment_events.json", "".join(events))]:
        (tmp_path / folder).mkdir(exist_ok=True)
        (tmp_path / folder / name).write_text(text)
    (tmp_path / "purchase_orders").mkdir()
    for i, text in enumerate(po_files):
        p = tmp_path / "purchase_orders" / f"purchase_orders_{i}.csv"
        p.write_text(PO_HDR + text)
        os.utime(p, (1_700_000_000 + i * 3600, 1_700_000_000 + i * 3600))   # delivery order
    return str(tmp_path)


def test_incremental_po_latest_version_wins_and_overlap_is_silent(spark, tmp_path):
    v1 = "PO-1,1,SUP-0001,PRD-00001,2025-10-01,2025-10-20,100,2.0,EUR,WH-MIL-01,OPEN,2025-10-01T10:00:00Z\n"
    v2 = "PO-1,1,SUP-0001,PRD-00001,2025-10-01,2025-10-20,150,2.0,EUR,WH-MIL-01,OPEN,2025-10-05T10:00:00Z\n"
    t = run_on_landing(spark, _landing(tmp_path, [v1, v2 + v1, v2], [EVT % (1, "CREATED", "2025-10-02T10:00:00Z")]))
    t = t["tables"]
    po = t["silver_purchase_orders"].collect()
    assert len(po) == 1 and str(po[0].ordered_qty) == "150.00"
    assert t["ops_dq_issues"].where("entity = 'purchase_orders'").count() == 0


def test_conflicting_same_timestamp_versions_are_quarantined(spark, tmp_path):
    a = "PO-1,1,SUP-0001,PRD-00001,2025-10-01,2025-10-20,100,2.0,EUR,WH-MIL-01,OPEN,2025-10-01T10:00:00Z\n"
    b = "PO-1,1,SUP-0001,PRD-00001,2025-10-01,2025-10-20,999,2.0,EUR,WH-MIL-01,OPEN,2025-10-01T10:00:00Z\n"
    t = run_on_landing(spark, _landing(tmp_path, [a + b], [EVT % (1, "CREATED", "2025-10-02T10:00:00Z")]))["tables"]
    assert [str(r.ordered_qty) for r in t["silver_purchase_orders"].collect()] == ["100.00"]
    assert [r.rule for r in t["silver_quarantine"].where("entity = 'purchase_orders'").collect()] == \
        ["duplicate_order_line"]


def test_zero_quantity_is_quarantined(spark, tmp_path):
    z = "PO-1,1,SUP-0001,PRD-00001,2025-10-01,2025-10-20,0,2.0,EUR,WH-MIL-01,OPEN,2025-10-01T10:00:00Z\n"
    t = run_on_landing(spark, _landing(tmp_path, [z], [EVT % (1, "CREATED", "2025-10-02T10:00:00Z")]))["tables"]
    assert t["silver_purchase_orders"].count() == 0
    assert [r.rule for r in t["silver_quarantine"].where("entity = 'purchase_orders'").collect()] == ["invalid_value"]


def test_shipment_lifecycle_rules(spark, tmp_path):
    po = "PO-1,1,SUP-0001,PRD-00001,2025-10-01,2025-10-20,100,2.0,EUR,WH-MIL-01,OPEN,2025-10-01T10:00:00Z\n"
    events = [EVT % (1, "CREATED", "2025-10-02T10:00:00+02:00"),
              EVT % (2, "DELIVERED", "2025-10-02T12:00:00+02:00"),     # before PICKED_UP -> invalid
              EVT % (3, "PICKED_UP", "2025-10-03T10:00:00+02:00"),
              EVT % (4, "DELIVERED", "2025-10-06T10:00:00+02:00"),     # valid delivery
              EVT % (5, "IN_TRANSIT", "2025-10-07T10:00:00+02:00"),    # after DELIVERED -> invalid
              EVT % (1, "CREATED", "2025-10-02T10:00:00+02:00")]       # exact duplicate
    t = run_on_landing(spark, _landing(tmp_path, [po], events))["tables"]
    kept = sorted(r.event_id for r in t["silver_shipment_events"].select("event_id").distinct().collect())
    assert kept == ["E1", "E3", "E4"]
    rules = {(r.record_key, r.rule) for r in t["ops_dq_issues"].where("entity = 'shipment_events'").collect()}
    assert rules == {("E2", "invalid_status_sequence"), ("E5", "invalid_status_sequence"), ("E1", "duplicate_event")}
    assert t["silver_quarantine"].where(F.col("entity") == "shipment_events").count() == 3
