"""
Gold tests.
  1. Star schema exactly as the Team 4 assignment table (3 facts, 6 dimensions), keys, integrity, docs.
  2. The 8 brief KPIs evaluated like the DAX measures (src/gold/config.py) equal an independent plain-Python
     implementation of business_brief.md section 4.
  3. Hand-made scenarios with hand-computed values for every derived field.
"""
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from gold import transforms as T
from gold.docs import TABLES, column_doc
from silver.reference import FX_RATES

AS_OF = date(2026, 10, 5)
FACTS = ["gold_fact_purchase_order_line", "gold_fact_shipment_event", "gold_fact_inventory_exposure"]
DIMS = {"gold_dim_date": "date_key", "gold_dim_supplier": "supplier_key", "gold_dim_product": "product_key",
        "gold_dim_location": "location_key", "gold_dim_shipment_status": "shipment_status_key",
        "gold_dim_risk_level": "risk_level_key"}


# =========================================================================== 1. model
def test_gold_tables_are_exactly_the_assignment_model(tables):
    assert sorted(t for t in tables if t.startswith("gold_")) == sorted(FACTS + list(DIMS))


def test_dimension_keys_unique_with_unknown_member(tables):
    for dim, key in DIMS.items():
        df = tables[dim]
        assert df.count() == df.select(key).distinct().count(), dim
        assert df.where(F.col(key) == -1).count() == 1, dim


def test_referential_integrity(tables):
    for fact in FACTS:
        for c in tables[fact].columns:
            dim = {"date": "gold_dim_date", "supplier_key": "gold_dim_supplier", "product_key": "gold_dim_product",
                   "location_key": "gold_dim_location", "shipment_status_key": "gold_dim_shipment_status",
                   "risk_level_key": "gold_dim_risk_level"}.get("date" if c.endswith("date_key") else c)
            if dim:
                f, d = tables[fact], tables[dim]
                assert f.join(d, f[c] == d[DIMS[dim]], "left_anti").count() == 0, f"{fact}.{c}"


def test_grains_and_silver_reconciliation(tables):
    po, fpo = tables["silver_purchase_orders"], tables["gold_fact_purchase_order_line"]
    assert fpo.count() == po.where("line_status <> 'CANCELLED'").count() == 3798
    assert fpo.count() == fpo.select("po_id", "po_line_number").distinct().count()
    total = F.sum(F.col("ordered_qty") * F.col("unit_price") * F.col("rate_to_eur"))
    assert fpo.agg(total).first()[0] == po.where("line_status <> 'CANCELLED'").agg(total).first()[0]
    assert tables["gold_fact_shipment_event"].count() == \
        tables["silver_shipment_events"].select("event_id").distinct().count() == 9294
    assert tables["gold_fact_inventory_exposure"].count() == tables["silver_products"].count() == 300


def test_every_gold_column_is_documented_with_sensitivity(tables):
    for t in TABLES:
        for f in tables[t].schema.fields:
            assert column_doc(t, f.name), f"{t}.{f.name} undocumented"
            assert f.metadata["comment"].endswith(("[Internal]", "[Public]", "[Confidential]")), f"{t}.{f.name}"


# =========================================================================== 2. KPIs
def gold_kpis(t):
    """The DAX measures of src/gold/config.py, evaluated with Spark."""
    fpo, exp, prod = t["gold_fact_purchase_order_line"], t["gold_fact_inventory_exposure"], t["gold_dim_product"]
    snap_key = exp.agg(F.max("snapshot_date_key")).first()[0]
    due = fpo.where(F.col("promised_delivery_date_key") <= snap_key)
    exposed = exp.where("days_of_cover IS NOT NULL AND days_until_arrival IS NOT NULL "
                        "AND days_of_cover < days_until_arrival")
    crit = prod.where("criticality = 'CRITICAL'")
    return {
        "Order Value": fpo.agg(F.sum(F.col("ordered_qty") * F.col("unit_price") * F.col("rate_to_eur"))).first()[0],
        "On-Time Delivery Rate": fpo.where("is_on_time").count() / fpo.where("is_delivered").count(),
        "Average Delay (days)": fpo.where("is_delivered AND delay_days > 0").agg(F.avg("delay_days")).first()[0],
        "Supplier Fill Rate": due.agg(F.sum(F.least("received_qty", "ordered_qty")) / F.sum("ordered_qty")).first()[0],
        "Exposed Value": exposed.agg(F.sum(F.col("delayed_inbound_qty") * F.col("unit_cost")
                                           * F.col("rate_to_eur"))).first()[0],
        "Delayed Quantity": fpo.agg(F.sum("delayed_qty")).first()[0],
        "Critical-Item Count": exp.join(crit, "product_key").where("delayed_inbound_qty > 0")
                                  .select("product_key").distinct().count(),
        "Alternate-Supplier Coverage": crit.where("has_active_alternate").count() / crit.count(),
    }


def _d(x):
    return Decimal(str(x)) if x is not None else None


def _rate(currency, on):
    rates = sorted((vf, r) for c, vf, r in FX_RATES if c == currency and vf <= on)
    return Decimal(str(rates[-1][1])) if rates else None


def brief_reference(t, snap):
    """business_brief.md section 4, implemented literally in plain Python from the Silver tables."""
    po = [r.asDict() for r in t["silver_purchase_orders"].collect() if r.line_status != "CANCELLED"]
    ev = defaultdict(list)
    for r in t["silver_shipment_events"].collect():
        ev[(r.po_id, r.po_line_number)].append(r)
    products = {r.product_id: r.asDict() for r in t["silver_products"].collect()}
    suppliers = {r.supplier_id: r.asDict() for r in t["silver_suppliers"].collect()}
    out, lines = defaultdict(Decimal), []
    for p in po:
        rows = ev[(p["po_id"], p["po_line_number"])]
        deliv = [r for r in rows if r.status == "DELIVERED"]
        received = sum((_d(r.quantity) for r in deliv), Decimal(0))
        by_promised = sum((_d(r.quantity) for r in deliv if r.event_ts.date() <= p["promised_delivery_date"]),
                          Decimal(0))
        actual = max((r.event_ts.date() for r in deliv), default=None)
        latest = max(rows, key=lambda r: r.event_ts, default=None)
        latest_eta = latest.eta_ts.date() if latest and latest.eta_ts else None
        ordered, promised = _d(p["ordered_qty"]), p["promised_delivery_date"]
        delivered = received >= ordered or p["line_status"] == "CLOSED"
        past_due = promised <= snap
        delay = ((actual - promised).days if actual else None) if delivered else \
            (snap - promised).days if past_due else 0
        late_eta = latest_eta is not None and latest_eta > promised
        delayed = (delivered and (delay or 0) > 0) or (not delivered and (past_due or late_eta))
        dq = max(ordered - by_promised, Decimal(0)) if past_due else \
            max(ordered - received, Decimal(0)) if late_eta else Decimal(0)
        out["ov"] += ordered * _d(p["unit_price"]) * _d(p["rate_to_eur"])
        out["delivered"] += delivered
        out["on_time"] += delivered and actual is not None and actual <= promised
        if delivered and (delay or 0) > 0:
            out["late_days"] += delay
            out["late"] += 1
        if past_due:
            out["fill_n"] += min(received, ordered)
            out["fill_d"] += ordered
        out["dq"] += dq
        lines.append(dict(p, dq=dq, open_delayed=(not delivered) and delayed, eta=latest_eta, delay=delay))
    exposed, crit, crit_delayed, crit_alt = Decimal(0), 0, 0, 0
    for pid, prod in products.items():
        od = sorted([ln for ln in lines if ln["product_id"] == pid and ln["open_delayed"]],
                    key=lambda ln: (ln["promised_delivery_date"], ln["po_id"], ln["po_line_number"]))
        inbound = sum((ln["dq"] for ln in od), Decimal(0))
        demand, cover, until = _d(prod["avg_daily_demand"]), None, None
        if demand and demand > 0 and prod["on_hand_qty"] is not None and prod["safety_stock_qty"] is not None:
            cover = (max(Decimal(0), _d(prod["on_hand_qty"]) - _d(prod["safety_stock_qty"])) / demand) \
                .quantize(Decimal("0.01"))
        if od:
            e = od[0]
            until = (e["eta"] - snap).days if e["eta"] else e["delay"] + suppliers[e["supplier_id"]][
                "contract_lead_time_days"]
        if inbound > 0 and cover is not None and until is not None and cover < until:
            exposed += inbound * _d(prod["unit_cost"]) * _rate(prod["cost_currency"], snap)
        alt = prod["alternate_supplier_id"]
        active_alt = alt is not None and alt != prod["primary_supplier_id"] and \
            suppliers.get(alt, {}).get("is_active") is True
        if prod["criticality"] == "CRITICAL":
            crit += 1
            crit_delayed += inbound > 0
            crit_alt += active_alt
    return {"Order Value": out["ov"], "On-Time Delivery Rate": out["on_time"] / out["delivered"],
            "Average Delay (days)": Decimal(out["late_days"]) / out["late"],
            "Supplier Fill Rate": out["fill_n"] / out["fill_d"], "Exposed Value": exposed,
            "Delayed Quantity": out["dq"], "Critical-Item Count": crit_delayed,
            "Alternate-Supplier Coverage": Decimal(crit_alt) / crit}


def test_brief_kpis_equal_independent_reference(tables):
    got, ref = gold_kpis(tables), brief_reference(tables, AS_OF)
    for kpi in ref:
        assert abs(float(got[kpi]) - float(ref[kpi])) < 0.01, f"{kpi}: gold={got[kpi]} brief={ref[kpi]}"
    print("\nBrief KPIs at", AS_OF, {k: round(float(v), 4) for k, v in got.items()})


# =========================================================================== 3. scenarios
PO_SCHEMA = ("po_id STRING, po_line_number INT, supplier_id STRING, product_id STRING, order_date DATE, "
             "promised_delivery_date DATE, ordered_qty DECIMAL(18,2), unit_price DECIMAL(18,4), currency STRING, "
             "rate_to_eur DECIMAL(18,8), delivery_location_id STRING, line_status STRING")
EV_SCHEMA = ("event_id STRING, shipment_id STRING, po_id STRING, po_line_number INT, product_id STRING, "
             "status STRING, event_ts TIMESTAMP, eta_ts TIMESTAMP, quantity DECIMAL(18,2)")
PRD_SCHEMA = ("product_id STRING, criticality STRING, unit_cost DECIMAL(18,4), cost_currency STRING, "
              "primary_supplier_id STRING, alternate_supplier_id STRING, stocking_location_id STRING, "
              "on_hand_qty DECIMAL(18,2), safety_stock_qty DECIMAL(18,2), avg_daily_demand DECIMAL(18,2)")
SUP_SCHEMA = "supplier_id STRING, contract_lead_time_days INT, is_active BOOLEAN, risk_rating STRING"
D, DT = date, datetime


def _po(i, product, promised, status):
    return (f"PO-{i}", 1, "S1", product, D(2026, 8, 1), promised, Decimal(100), Decimal(2), "EUR", Decimal(1),
            "WH-1", status)


def _ev(e, line, qty, status, ts, eta):
    return (e, f"SH-{e}", f"PO-{line}", 1, "P", status, ts, eta, Decimal(qty))


@pytest.fixture(scope="module")
def scenario(spark):
    po = spark.createDataFrame([
        _po(1, "P2", D(2026, 9, 5), "CLOSED"),               # delivered on time
        _po(2, "P2", D(2026, 9, 5), "CLOSED"),               # delivered 5 days late
        _po(3, "P2", D(2026, 9, 5), "CLOSED"),               # closed short (60 of 100), on time
        _po(4, "P1", D(2026, 9, 20), "PARTIALLY_RECEIVED"),  # open, past due, 30 received
        _po(5, "P1", D(2026, 10, 20), "OPEN"),               # not due, ETA after promised
        _po(6, "P2", D(2026, 10, 20), "OPEN"),               # not due, ETA before promised
        _po(7, "P2", D(2026, 9, 5), "CANCELLED"),            # excluded
    ], PO_SCHEMA)
    ev = spark.createDataFrame([
        _ev("E1", 1, 100, "DELIVERED", DT(2026, 9, 1, 10), DT(2026, 9, 1)),
        _ev("E2", 2, 100, "DELIVERED", DT(2026, 9, 10, 10), DT(2026, 9, 10)),
        _ev("E3", 3, 60, "DELIVERED", DT(2026, 9, 1, 10), DT(2026, 9, 1)),
        _ev("E4a", 4, 30, "DELIVERED", DT(2026, 9, 1, 10), DT(2026, 9, 1)),
        _ev("E4b", 4, 70, "IN_TRANSIT", DT(2026, 10, 1, 10), DT(2026, 10, 12, 8)),
        _ev("E5", 5, 100, "CREATED", DT(2026, 10, 1, 10), DT(2026, 10, 25)),
        _ev("E6", 6, 100, "CREATED", DT(2026, 10, 1, 10), DT(2026, 10, 18)),
    ], EV_SCHEMA)
    products = spark.createDataFrame([
        ("P1", "CRITICAL", Decimal(2), "EUR", "S1", "S2", "WH-1", Decimal(50), Decimal(10), Decimal(10)),
        ("P2", "HIGH", Decimal(1), "EUR", "S1", None, "WH-1", Decimal(1000), Decimal(10), Decimal(10)),
        ("P3", "CRITICAL", Decimal(1), "EUR", "S2", "S1", "WH-1", Decimal(10), None, Decimal(1)),
    ], PRD_SCHEMA)
    suppliers = spark.createDataFrame([("S1", 10, True, "LOW"), ("S2", 20, False, "HIGH")], SUP_SCHEMA)
    fx = spark.createDataFrame([("EUR", D(2000, 1, 1), Decimal(1))], "currency STRING, valid_from DATE, "
                                                                      "rate_to_eur DECIMAL(18,8)")
    lines = {r.po_id: r for r in T.po_line_kpis(po, ev, D(2026, 10, 5)).collect()}
    exposure = {r.product_id: r for r in
                T.product_exposure(T.po_line_kpis(po, ev, D(2026, 10, 5)), products, suppliers, fx,
                                   D(2026, 10, 5)).collect()}
    alt = {r.product_id: r.has_active_alternate for r in T.has_active_alternate(products, suppliers).collect()}
    return lines, exposure, alt


@pytest.mark.parametrize("po_id, delivered, on_time, delay, delayed, delayed_qty, received", [
    ("PO-1", True, True, -4, False, 0, 100),
    ("PO-2", True, False, 5, True, 100, 100),       # nothing received by the promised date
    ("PO-3", True, True, -4, False, 40, 60),        # CLOSED counts as delivered; 40 never arrived (past due)
    ("PO-4", False, False, 15, True, 70, 30),       # 2026-10-05 - 2026-09-20
    ("PO-5", False, False, 0, True, 100, 0),        # not due, ETA 10-25 > promised 10-20
    ("PO-6", False, False, 0, False, 0, 0),         # not due, ETA before promised
])
def test_po_line_derived_fields(scenario, po_id, delivered, on_time, delay, delayed, delayed_qty, received):
    r = scenario[0][po_id]
    assert (r.is_delivered, r.is_on_time, r.delay_days, r.is_delayed) == (delivered, on_time, delay, delayed)
    assert r.delayed_qty == delayed_qty and r.received_qty == received


def test_cancelled_lines_excluded(scenario):
    assert "PO-7" not in scenario[0]


def test_product_exposure(scenario):
    e = scenario[1]["P1"]
    assert e.delayed_inbound_qty == 170                    # PO-4 70 + PO-5 100
    assert e.days_of_cover == 4.0                          # (50 - 10) / 10
    assert e.days_until_arrival == 7                       # ETA of PO-4 (earliest promised) 10-12 - 10-05
    assert scenario[1]["P2"].delayed_inbound_qty == 0 and scenario[1]["P2"].days_until_arrival is None
    assert scenario[1]["P3"].days_of_cover is None         # unknown safety stock -> unknown cover, not 0
    assert scenario[2] == {"P1": False, "P2": False, "P3": True}   # S2 inactive; P3 alt S1 active & != primary
