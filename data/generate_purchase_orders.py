"""Generate purchase_orders.csv and purchase_orders_errors.csv. Requires suppliers.csv and products.csv."""
import argparse
import csv
import random
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

N_LINES = 4000
RULE_RATE = 0.005  # share of rows per data-quality rule (minimum 1 row)
ORDER_START = date(2025, 10, 1)
SNAPSHOT_DATE = date(2026, 9, 30)

FX_RATE_TO_EUR = {
    "EUR": 1.0, "USD": 0.92, "GBP": 1.17, "CHF": 1.04, "PLN": 0.23,
    "CNY": 0.127, "JPY": 0.0061, "INR": 0.011, "KRW": 0.00068, "MXN": 0.05,
}
COUNTRY_CURRENCY = {
    "DE": "EUR", "FR": "EUR", "IT": "EUR", "NL": "EUR", "ES": "EUR", "GR": "EUR",
    "PL": "PLN", "GB": "GBP", "CH": "CHF", "CN": "CNY", "JP": "JPY", "IN": "INR",
    "KR": "KRW", "TW": "USD", "VN": "USD", "US": "USD", "MX": "MXN",
}
WAREHOUSES = ["WH-ATH-01", "WH-THE-01", "WH-RTM-01", "WH-HAM-01", "WH-MIL-01"]
INVALID_CURRENCIES = ["EURO", "usd", "US$", "€", "XXX"]

FIELDS = [
    "po_id", "po_line_number", "supplier_id", "product_id", "order_date",
    "promised_delivery_date", "ordered_qty", "unit_price", "currency",
    "delivery_location_id", "line_status", "last_modified_ts",
]


def n_defects(n_rows, rate):
    return max(1, round(n_rows * rate))


def pick_rows(rng, rows, count, used):
    candidates = [i for i in range(len(rows)) if i not in used]
    chosen = rng.sample(candidates, min(count, len(candidates)))
    used.update(chosen)
    return chosen


def load_first_by_key(path, key):
    result = {}
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            result.setdefault(row[key], row)
    return result


def ts(day, rng):
    moment = datetime.combine(day, time(rng.randint(6, 20), rng.randint(0, 59), rng.randint(0, 59)))
    return moment.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def supplier_currency(supplier):
    if supplier["default_currency"] in FX_RATE_TO_EUR:
        return supplier["default_currency"]
    return COUNTRY_CURRENCY.get(supplier["country_code"], "EUR")


def line_status(rng, promised):
    if rng.random() < 0.03:
        return "CANCELLED"
    days_past_due = (SNAPSHOT_DATE - promised).days
    if days_past_due > 10:
        options = {"CLOSED": 88, "PARTIALLY_RECEIVED": 6, "OPEN": 6}
    elif days_past_due >= 0:
        options = {"CLOSED": 50, "PARTIALLY_RECEIVED": 15, "OPEN": 35}
    else:
        options = {"OPEN": 95, "PARTIALLY_RECEIVED": 5}
    return rng.choices(list(options), list(options.values()))[0]


def last_modified(rng, status, order_date, promised):
    if status == "CLOSED":
        day = promised + timedelta(days=rng.randint(-3, 10))
    elif status == "PARTIALLY_RECEIVED":
        day = promised + timedelta(days=rng.randint(-5, 5))
    elif status == "CANCELLED":
        day = order_date + timedelta(days=rng.randint(1, 10))
    else:
        day = order_date + timedelta(days=rng.randint(0, 3))
    return ts(min(max(day, order_date), SNAPSHOT_DATE), rng)


def generate(rng, suppliers, products):
    catalog = {}
    for p in products.values():
        for s in (p["primary_supplier_id"], p["alternate_supplier_id"]):
            if s in suppliers:
                catalog.setdefault(s, []).append(p)

    supplier_ids = [s for s, r in suppliers.items() if r["is_active"] == "true" or rng.random() < 0.3]
    weights = [len(catalog.get(s, [])) + 1 for s in supplier_ids]
    all_products = list(products.values())
    span = (SNAPSHOT_DATE - ORDER_START).days

    orders, total = [], 0
    while total < N_LINES:
        n_lines = rng.choices([1, 2, 3, 4, 5], [20, 30, 25, 15, 10])[0]
        orders.append((ORDER_START + timedelta(days=rng.randint(0, span)),
                       rng.choices(supplier_ids, weights)[0], n_lines))
        total += n_lines
    orders.sort(key=lambda o: o[0])

    rows = []
    for seq, (order_date, supplier_id, n_lines) in enumerate(orders, start=1):
        supplier = suppliers[supplier_id]
        currency = supplier_currency(supplier)
        own = catalog.get(supplier_id, [])
        chosen = rng.sample(own, min(n_lines, len(own)))
        while len(chosen) < n_lines:
            candidate = rng.choice(all_products)
            if candidate not in chosen:
                chosen.append(candidate)

        location = chosen[0]["stocking_location_id"] if rng.random() < 0.85 else rng.choice(WAREHOUSES)
        lead = int(supplier["contract_lead_time_days"])
        promised = order_date + timedelta(days=max(3, lead + rng.randint(-2, 4)))
        po_status = line_status(rng, promised)
        for line_number, product in enumerate(chosen, start=1):
            demand = float(product["avg_daily_demand"])
            quantity = max(1.0, demand * rng.uniform(10, 45))
            quantity = round(quantity) if product["unit_of_measure"] == "EA" else round(quantity, 2)
            cost_eur = float(product["unit_cost"]) * FX_RATE_TO_EUR.get(product["cost_currency"], 1.0)
            price = cost_eur / FX_RATE_TO_EUR[currency] * rng.uniform(1.02, 1.25)
            status = po_status if rng.random() < 0.9 else line_status(rng, promised)

            rows.append({
                "po_id": f"PO-{order_date.year}-{seq:05d}",
                "po_line_number": line_number,
                "supplier_id": supplier_id,
                "product_id": product["product_id"],
                "order_date": order_date.isoformat(),
                "promised_delivery_date": promised.isoformat(),
                "ordered_qty": f"{quantity:.2f}",
                "unit_price": f"{price:.4f}",
                "currency": currency,
                "delivery_location_id": location,
                "line_status": status,
                "last_modified_ts": last_modified(rng, status, order_date, promised),
            })
    return rows[:N_LINES]


def inject_defects(rng, rows, suppliers):
    used = set()
    n = n_defects(len(rows), RULE_RATE)

    for i in pick_rows(rng, rows, n, used):
        rows[i]["supplier_id"] = f"SUP-{rng.randint(9001, 9999)}"
        rows[i]["_dq_rule"] = "unknown_supplier"
        rows[i]["_dq_description"] = f"supplier_id {rows[i]['supplier_id']} does not exist in suppliers.csv"

    for i in pick_rows(rng, rows, n, used):
        rows[i]["product_id"] = f"PRD-{rng.randint(90001, 99999)}"
        rows[i]["_dq_rule"] = "unknown_product"
        rows[i]["_dq_description"] = f"product_id {rows[i]['product_id']} does not exist in products.csv"

    for i in pick_rows(rng, rows, n, used):
        rows[i]["ordered_qty"] = f"-{rows[i]['ordered_qty']}"
        rows[i]["_dq_rule"] = "negative_quantity"
        rows[i]["_dq_description"] = f"ordered_qty is negative ({rows[i]['ordered_qty']})"

    for i in pick_rows(rng, rows, n, used):
        order_date = date.fromisoformat(rows[i]["order_date"])
        rows[i]["promised_delivery_date"] = (order_date - timedelta(days=rng.randint(1, 30))).isoformat()
        rows[i]["_dq_rule"] = "delivery_before_order"
        rows[i]["_dq_description"] = (
            f"promised_delivery_date {rows[i]['promised_delivery_date']} is before order_date {rows[i]['order_date']}"
        )

    for i in pick_rows(rng, rows, n, used):
        expected = rows[i]["currency"]
        supplier_default = suppliers.get(rows[i]["supplier_id"], {}).get("default_currency")
        if supplier_default not in FX_RATE_TO_EUR or rng.random() < 0.5:
            rows[i]["currency"] = rng.choice(INVALID_CURRENCIES)
            reason = "is not a valid ISO 4217 code"
        else:
            rows[i]["currency"] = rng.choice([c for c in FX_RATE_TO_EUR if c != expected])
            reason = f"differs from supplier default_currency {expected}"
        rows[i]["_dq_rule"] = "currency_inconsistency"
        rows[i]["_dq_description"] = f"currency '{rows[i]['currency']}' {reason}"

    inserts = {}
    for i in pick_rows(rng, rows, n, used):
        dup = {k: v for k, v in rows[i].items() if not k.startswith("_")}
        key = f"({dup['po_id']}, {dup['po_line_number']})"
        if rng.random() < 0.5:
            dup["_dq_description"] = f"Exact duplicate of order line {key}"
        else:
            dup["ordered_qty"] = f"{float(dup['ordered_qty']) * rng.choice([0.5, 2, 3]):.2f}"
            dup["_dq_description"] = f"Duplicate order line {key} with conflicting ordered_qty and same last_modified_ts"
        dup["_dq_rule"] = "duplicate_order_line"
        inserts.setdefault(rng.randint(i, len(rows) - 1), []).append(dup)

    result = []
    for idx, row in enumerate(rows):
        result.append(row)
        result.extend(inserts.get(idx, []))
    return result


def write_outputs(rows, out_dir, name):
    with (out_dir / f"{name}.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    err_fields = FIELDS + ["source_line_number", "dq_rule", "dq_description"]
    errors = [
        {**{k: r[k] for k in FIELDS}, "source_line_number": line,
         "dq_rule": r["_dq_rule"], "dq_description": r["_dq_description"]}
        for line, r in enumerate(rows, start=2) if r.get("_dq_rule")
    ]
    with (out_dir / f"{name}_errors.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=err_fields)
        writer.writeheader()
        writer.writerows(errors)
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--seed", type=int, default=303)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    rng = random.Random(args.seed)
    suppliers = load_first_by_key(out_dir / "suppliers.csv", "supplier_id")
    products = load_first_by_key(out_dir / "products.csv", "product_id")

    rows = inject_defects(rng, generate(rng, suppliers, products), suppliers)
    errors = write_outputs(rows, out_dir, "purchase_orders")
    print(f"purchase_orders.csv: {len(rows)} rows | purchase_orders_errors.csv: {len(errors)} rows")


if __name__ == "__main__":
    main()
