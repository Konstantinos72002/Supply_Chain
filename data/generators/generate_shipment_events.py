"""Generate shipment_events.json (JSON Lines) and shipment_events_errors.csv.
Requires suppliers.csv, products.csv and purchase_orders.csv."""
import argparse
import copy
import csv
import json
import random
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

RULE_RATE = 0.004  # share of events per data-quality rule (minimum 1 event)
SNAPSHOT_DATE = date(2026, 9, 30)
SNAPSHOT_END = datetime.combine(SNAPSHOT_DATE, time(23, 59, 59), tzinfo=timezone.utc)

EU_COUNTRIES = {"DE", "FR", "IT", "NL", "ES", "GR", "PL"}
TZ_HOURS = {
    "DE": 1, "FR": 1, "IT": 1, "NL": 1, "ES": 1, "GR": 2, "PL": 1, "GB": 0, "CH": 1,
    "CN": 8, "JP": 9, "IN": 5.5, "KR": 9, "TW": 8, "VN": 7, "US": -5, "MX": -6,
}
WAREHOUSES = {
    "WH-ATH-01": ("Athens Distribution Center", "GR"),
    "WH-THE-01": ("Thessaloniki Distribution Center", "GR"),
    "WH-RTM-01": ("Rotterdam Distribution Center", "NL"),
    "WH-HAM-01": ("Hamburg Distribution Center", "DE"),
    "WH-MIL-01": ("Milan Distribution Center", "IT"),
}
DEST_PORT = {"GR": ("PORT-GRPIR", "Port of Piraeus"), "NL": ("PORT-NLRTM", "Port of Rotterdam"),
             "DE": ("PORT-DEHAM", "Port of Hamburg"), "IT": ("PORT-ITGOA", "Port of Genoa")}
DEST_HUB = {"GR": ("HUB-GR-ASP", "Aspropyrgos Hub"), "NL": ("HUB-NL-VNL", "Venlo Hub"),
            "DE": ("HUB-DE-HAM", "Hamburg Hub"), "IT": ("HUB-IT-PIA", "Piacenza Hub")}
ORIGIN_PORT = {
    "CN": ("PORT-CNSHA", "Port of Shanghai"), "JP": ("PORT-JPYOK", "Port of Yokohama"),
    "KR": ("PORT-KRPUS", "Port of Busan"), "IN": ("PORT-INNSA", "Nhava Sheva Port"),
    "TW": ("PORT-TWKHH", "Port of Kaohsiung"), "VN": ("PORT-VNSGN", "Port of Ho Chi Minh City"),
    "US": ("PORT-USNYC", "Port of New York"), "MX": ("PORT-MXVER", "Port of Veracruz"),
}
SEA_CARRIERS = ["Oceanic Lines", "BlueWave Shipping", "Pacific Star Cargo", "Atlas Maritime"]
ROAD_CARRIERS = ["EuroFreight", "TransAlp Logistics", "Rapid Road Cargo", "Continental Haulage"]
DELAY_REASONS = {
    "SUPPLIER_SITE": ["SUPPLIER_CAPACITY", "MATERIAL_SHORTAGE", "DOCUMENTATION"],
    "PORT": ["PORT_CONGESTION", "CUSTOMS_HOLD", "WEATHER", "VESSEL_DELAY"],
    "HUB": ["CARRIER_CAPACITY", "CUSTOMS_HOLD", "WEATHER"],
    "WAREHOUSE": ["CARRIER_CAPACITY", "DOCUMENTATION"],
}

ERROR_FIELDS = [
    "event_id", "shipment_id", "po_id", "status", "event_ts", "po_line_number", "product_id",
    "quantity", "source_line_number", "dq_rule", "dq_description",
]


def n_defects(n_rows, rate):
    return max(1, round(n_rows * rate))


def load_first_by_key(path, key_fn):
    result = {}
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            result.setdefault(key_fn(row), row)
    return result


def tz_for(country):
    return timezone(timedelta(hours=TZ_HOURS.get(country, 0)))


def loc(location_id, name, country, loc_type):
    return {"location_id": location_id, "name": name, "country": country, "type": loc_type}


def local_dt(day, country, rng, hours=(8, 17)):
    moment = datetime.combine(day, time(rng.randint(*hours), rng.randint(0, 59)), tzinfo=tz_for(country))
    return moment.astimezone(timezone.utc)


def sample_delay(rng):
    r = rng.random()
    if r < 0.68:
        return rng.randint(-4, 0)
    if r < 0.92:
        return rng.randint(1, 7)
    return rng.randint(8, 25)


def route_for(supplier_country):
    if supplier_country in EU_COUNTRIES:
        return "EU_ROAD"
    if supplier_country in ("GB", "CH"):
        return "ROAD_CUSTOMS"
    return "SEA"


def transit_range(route, supplier_country):
    if route == "EU_ROAD":
        return 2, 6
    if route == "ROAD_CUSTOMS":
        return 3, 8
    return (12, 20) if supplier_country in ("US", "MX") else (22, 35)


def plan_events(rng, ctx, arrival_day, keep_pickup_before_snapshot=False):
    """Return the full planned timeline [(status, ts_utc, location)] ending in DELIVERED."""
    s_country, wh_id = ctx["supplier_country"], ctx["warehouse_id"]
    wh_name, wh_country = WAREHOUSES.get(wh_id, ("Unknown Warehouse", "GR"))
    route = route_for(s_country)
    transit = rng.randint(*transit_range(route, s_country))
    transit = min(transit, max(1, (arrival_day - ctx["order_date"]).days - 1))
    pickup_day = arrival_day - timedelta(days=transit)
    if keep_pickup_before_snapshot and pickup_day >= SNAPSHOT_DATE:
        pickup_day = max(ctx["order_date"] + timedelta(days=1), SNAPSHOT_DATE - timedelta(days=rng.randint(1, 3)))
    created_day = max(ctx["order_date"], pickup_day - timedelta(days=rng.randint(1, 3)))

    site = loc(f"SITE-{ctx['supplier_id']}", f"{ctx['supplier_name']} Plant", s_country, "SUPPLIER_SITE")
    hub = loc(*DEST_HUB[wh_country], wh_country, "HUB")
    warehouse = loc(wh_id, wh_name, wh_country, "WAREHOUSE")
    created = local_dt(created_day, s_country, rng, (7, 11))
    pickup = max(local_dt(pickup_day, s_country, rng, (12, 18)), created + timedelta(hours=2))
    arrival = max(local_dt(arrival_day, wh_country, rng), pickup + timedelta(hours=20))
    span = arrival - pickup

    events = [("CREATED", created, site), ("PICKED_UP", pickup, site)]
    if route == "SEA":
        events += [
            ("IN_TRANSIT", pickup + span * 0.08, loc(*ORIGIN_PORT[s_country], s_country, "PORT")),
            ("IN_TRANSIT", pickup + span * 0.80, loc(*DEST_PORT[wh_country], wh_country, "PORT")),
            ("CUSTOMS", pickup + span * 0.88, loc(*DEST_PORT[wh_country], wh_country, "PORT")),
        ]
    else:
        events.append(("IN_TRANSIT", pickup + span * 0.40, hub))
        if route == "ROAD_CUSTOMS":
            events.append(("CUSTOMS", pickup + span * 0.60, hub))
    events += [
        ("OUT_FOR_DELIVERY", arrival - timedelta(hours=rng.randint(3, 8)), hub),
        ("DELIVERED", arrival, warehouse),
    ]
    return events, arrival


def build_shipment(rng, ctx, items, arrival_day, keep_pickup_before_snapshot=False):
    """Plan a shipment, add a DELAYED event when late, keep only events up to the snapshot."""
    planned, arrival = plan_events(rng, ctx, arrival_day, keep_pickup_before_snapshot)
    wh_country = WAREHOUSES.get(ctx["warehouse_id"], ("", "GR"))[1]
    promised_eta = datetime.combine(ctx["promised"], time(12, 0), tzinfo=tz_for(wh_country)).astimezone(timezone.utc)
    delayed_at = None

    if arrival_day > ctx["promised"] and planned[0][1] <= SNAPSHOT_END:
        pickup, created = planned[1][1], planned[0][1]
        delayed_at = min(pickup + (arrival - pickup) * 0.5, SNAPSHOT_END - timedelta(hours=rng.randint(1, 12)))
        delayed_at = max(delayed_at, created + timedelta(hours=1))
        location = [e for e in planned if e[1] <= delayed_at][-1][2]
        planned.append(("DELAYED", delayed_at, location))
        planned.sort(key=lambda e: e[1])

    events = []
    for status, moment, location in planned:
        if moment > SNAPSHOT_END:
            continue
        eta = arrival if (delayed_at is None or moment >= delayed_at) else promised_eta
        reason = rng.choice(DELAY_REASONS[location["type"]]) if status == "DELAYED" else None
        events.append({"status": status, "ts": moment, "location": location, "eta": eta,
                       "eta_country": wh_country, "items": copy.deepcopy(items), "reason": reason})
    return events


def build_cancelled(rng, ctx, items):
    site = loc(f"SITE-{ctx['supplier_id']}", f"{ctx['supplier_name']} Plant", ctx["supplier_country"], "SUPPLIER_SITE")
    wh_country = WAREHOUSES.get(ctx["warehouse_id"], ("", "GR"))[1]
    eta = datetime.combine(ctx["promised"], time(12, 0), tzinfo=tz_for(wh_country)).astimezone(timezone.utc)
    created = local_dt(ctx["order_date"] + timedelta(days=rng.randint(1, 3)), ctx["supplier_country"], rng)
    cancelled = created + timedelta(days=rng.randint(1, 5), hours=rng.randint(1, 8))
    return [{"status": s, "ts": t, "location": site, "eta": eta, "eta_country": wh_country,
             "items": copy.deepcopy(items), "reason": None}
            for s, t in (("CREATED", created), ("CANCELLED", cancelled)) if t <= SNAPSHOT_END]


def item(line, quantity, uom):
    quantity = round(quantity) if uom == "EA" else round(quantity, 2)
    return {"po_line_number": int(line["po_line_number"]), "product_id": line["product_id"], "quantity": float(quantity)}


def generate(rng, suppliers, products, po_lines):
    groups = {}
    for line in po_lines.values():
        groups.setdefault((line["po_id"], line["line_status"], line["delivery_location_id"]), []).append(line)

    shipments = []
    for (po_id, status, warehouse_id), lines in groups.items():
        first = lines[0]
        supplier = suppliers.get(first["supplier_id"])
        order_date = date.fromisoformat(first["order_date"])
        promised = date.fromisoformat(first["promised_delivery_date"])
        if promised <= order_date:
            promised = order_date + timedelta(days=int(supplier["contract_lead_time_days"]) if supplier else 14)
        ctx = {
            "po_id": po_id, "order_date": order_date, "promised": promised, "warehouse_id": warehouse_id,
            "supplier_id": first["supplier_id"],
            "supplier_name": supplier["supplier_name"] if supplier else "Unregistered Supplier",
            "supplier_country": supplier["country_code"] if supplier else "DE",
            "carrier": None,
        }
        ctx["carrier"] = rng.choice(SEA_CARRIERS if route_for(ctx["supplier_country"]) == "SEA" else ROAD_CARRIERS)
        uom = {l["product_id"]: products.get(l["product_id"], {}).get("unit_of_measure", "EA") for l in lines}
        qty = {l["po_line_number"]: abs(float(l["ordered_qty"])) or 1.0 for l in lines}

        if status == "CANCELLED":
            if rng.random() < 0.4:
                shipments.append((ctx, build_cancelled(rng, ctx, [item(l, qty[l["po_line_number"]], uom[l["product_id"]]) for l in lines])))
            continue

        if status == "CLOSED":
            factor = rng.choices([1.0, rng.uniform(0.90, 0.99), rng.uniform(1.01, 1.05)], [95, 3, 2])[0]
            items = [item(l, qty[l["po_line_number"]] * factor, uom[l["product_id"]]) for l in lines]
            arrival = promised + timedelta(days=sample_delay(rng))
            arrival = max(order_date + timedelta(days=2), min(arrival, SNAPSHOT_DATE - timedelta(days=rng.randint(0, 2))))
            shipments.append((ctx, build_shipment(rng, ctx, items, arrival)))
            continue

        if status == "PARTIALLY_RECEIVED":
            share = rng.uniform(0.3, 0.8)
            delivered = [item(l, qty[l["po_line_number"]] * share, uom[l["product_id"]]) for l in lines]
            remaining = [item(l, qty[l["po_line_number"]] * (1 - share), uom[l["product_id"]]) for l in lines]
            arrival = promised + timedelta(days=sample_delay(rng))
            arrival = max(order_date + timedelta(days=2), min(arrival, SNAPSHOT_DATE - timedelta(days=rng.randint(0, 3))))
            shipments.append((ctx, build_shipment(rng, ctx, delivered, arrival)))
            if promised <= SNAPSHOT_DATE:
                next_arrival = SNAPSHOT_DATE + timedelta(days=rng.randint(1, 15))
            else:
                next_arrival = max(SNAPSHOT_DATE + timedelta(days=1), promised + timedelta(days=sample_delay(rng)))
            shipments.append((dict(ctx), build_shipment(rng, ctx, remaining, next_arrival, True)))
            continue

        items = [item(l, qty[l["po_line_number"]], uom[l["product_id"]]) for l in lines]
        if promised <= SNAPSHOT_DATE:
            arrival = SNAPSHOT_DATE + timedelta(days=rng.randint(1, 20))
            shipments.append((ctx, build_shipment(rng, ctx, items, arrival, rng.random() < 0.75)))
        else:
            late = rng.random() < 0.3
            arrival = promised + timedelta(days=rng.randint(1, 15) if late else rng.randint(-3, 0))
            arrival = max(arrival, SNAPSHOT_DATE + timedelta(days=1))
            shipments.append((ctx, build_shipment(rng, ctx, items, arrival)))

    events = []
    for number, (ctx, shipment_events) in enumerate((s for s in shipments if s[1]), start=1):
        for event in shipment_events:
            event.update({"shipment_id": f"SHP-{number:06d}", "po_id": ctx["po_id"],
                          "carrier": ctx["carrier"], "order_date": ctx["order_date"]})
            events.append(event)
    events.sort(key=lambda e: e["ts"])
    for number, event in enumerate(events, start=1):
        event["event_id"] = f"EVT-{number:07d}"
    return events


def inject_defects(rng, events, products):
    n = n_defects(len(events), RULE_RATE)
    inherited = {e["shipment_id"] for e in events if any(i["product_id"] not in products for i in e["items"])}
    used_shipments = set(inherited)
    next_id = len(events) + 1
    inserts = {}

    def pick(predicate, count):
        candidates = [i for i, e in enumerate(events) if e["shipment_id"] not in used_shipments and predicate(e)]
        chosen = rng.sample(candidates, min(count, len(candidates)))
        used_shipments.update(events[i]["shipment_id"] for i in chosen)
        return chosen

    def flag(event, rule, description, item_index=0):
        event["_dq_rule"], event["_dq_description"], event["_dq_item"] = rule, description, item_index

    for i in pick(lambda e: e["status"] != "CANCELLED", n):
        k = rng.randrange(len(events[i]["items"]))
        events[i]["items"][k]["product_id"] = f"PRD-{rng.randint(90001, 99999)}"
        flag(events[i], "unknown_product", f"items[{k}].product_id {events[i]['items'][k]['product_id']} does not exist in products.csv", k)

    for i in pick(lambda e: True, n):
        k = rng.randrange(len(events[i]["items"]))
        events[i]["items"][k]["quantity"] = -abs(events[i]["items"][k]["quantity"]) or -1.0
        flag(events[i], "negative_quantity", f"items[{k}].quantity is negative ({events[i]['items'][k]['quantity']})", k)

    by_shipment = {}
    for i, e in enumerate(events):
        by_shipment.setdefault(e["shipment_id"], {})[e["status"]] = i

    for i in pick(lambda e: e["status"] == "DELIVERED" and "PICKED_UP" in by_shipment[e["shipment_id"]], n):
        statuses = by_shipment[events[i]["shipment_id"]]
        if rng.random() < 0.5:
            created, picked = events[statuses["CREATED"]]["ts"], events[statuses["PICKED_UP"]]["ts"]
            events[i]["ts"] = created + (picked - created) / 2
            flag(events[i], "invalid_status_sequence", "DELIVERED event occurs before PICKED_UP for the same shipment")
        else:
            extra = copy.deepcopy({k: v for k, v in events[i].items() if not k.startswith("_")})
            extra.update({"event_id": f"EVT-{next_id:07d}", "status": "IN_TRANSIT", "reason": None,
                          "ts": events[i]["ts"] + timedelta(hours=rng.randint(2, 30))})
            next_id += 1
            flag(extra, "invalid_status_sequence", "IN_TRANSIT event occurs after DELIVERED for the same shipment")
            inserts.setdefault(i, []).append(extra)

    for i in pick(lambda e: e["status"] == "DELIVERED", n):
        new_day = events[i]["order_date"] - timedelta(days=rng.randint(1, 15))
        events[i]["ts"] = datetime.combine(new_day, events[i]["ts"].time(), tzinfo=timezone.utc)
        flag(events[i], "delivery_before_order",
             f"DELIVERED event_ts is before PO order_date {events[i]['order_date'].isoformat()} "
             "(also breaks the status sequence)")

    for i in pick(lambda e: True, n):
        dup = copy.deepcopy({k: v for k, v in events[i].items() if not k.startswith("_")})
        flag(dup, "duplicate_event", f"Exact duplicate of event_id {dup['event_id']}")
        position = min(len(events) - 1, i + rng.randint(0, 50))
        inserts.setdefault(position, []).append(dup)

    for event in events:
        if event["shipment_id"] in inherited:
            k = next(k for k, i in enumerate(event["items"]) if i["product_id"] not in products)
            flag(event, "unknown_product",
                 f"items[{k}].product_id {event['items'][k]['product_id']} does not exist in products.csv "
                 "(inherited from purchase order line)", k)

    result = []
    for idx, event in enumerate(events):
        result.append(event)
        result.extend(inserts.get(idx, []))
    return result


def to_json(event):
    return {
        "event_id": event["event_id"],
        "shipment_id": event["shipment_id"],
        "po_id": event["po_id"],
        "carrier": event["carrier"],
        "status": event["status"],
        "event_ts": event["ts"].astimezone(tz_for(event["location"]["country"])).isoformat(timespec="seconds"),
        "location": event["location"],
        "eta_ts": event["eta"].astimezone(tz_for(event["eta_country"])).isoformat(timespec="seconds"),
        "items": event["items"],
        "delay_reason_code": event["reason"],
    }


def write_outputs(events, out_dir):
    errors = []
    with (out_dir / "shipment_events.json").open("w", encoding="utf-8") as f:
        for line_number, event in enumerate(events, start=1):
            record = to_json(event)
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            if event.get("_dq_rule"):
                bad_item = record["items"][event["_dq_item"]]
                errors.append({
                    "event_id": record["event_id"], "shipment_id": record["shipment_id"],
                    "po_id": record["po_id"], "status": record["status"], "event_ts": record["event_ts"],
                    "po_line_number": bad_item["po_line_number"], "product_id": bad_item["product_id"],
                    "quantity": bad_item["quantity"], "source_line_number": line_number,
                    "dq_rule": event["_dq_rule"], "dq_description": event["_dq_description"],
                })
    with (out_dir / "shipment_events_errors.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ERROR_FIELDS)
        writer.writeheader()
        writer.writerows(errors)
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--seed", type=int, default=404)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    rng = random.Random(args.seed)
    suppliers = load_first_by_key(out_dir / "suppliers.csv", lambda r: r["supplier_id"])
    products = load_first_by_key(out_dir / "products.csv", lambda r: r["product_id"])
    po_lines = load_first_by_key(out_dir / "purchase_orders.csv", lambda r: (r["po_id"], r["po_line_number"]))

    events = inject_defects(rng, generate(rng, suppliers, products, po_lines), products)
    errors = write_outputs(events, out_dir)
    print(f"shipment_events.json: {len(events)} events | shipment_events_errors.csv: {len(errors)} rows")


if __name__ == "__main__":
    main()
