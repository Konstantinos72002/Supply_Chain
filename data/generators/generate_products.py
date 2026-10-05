"""Generate products.csv and products_errors.csv. Requires suppliers.csv."""
import argparse
import csv
import random
from pathlib import Path

N_PRODUCTS = 300
RULE_RATE = 0.006  # share of rows per data-quality rule (minimum 1 row)

# category: (unit_of_measure, unit_cost range, product nouns)
CATEGORIES = {
    "Electronic Components": ("EA", (0.40, 85.0), ["Capacitor", "Microcontroller", "Sensor Module", "Power Regulator", "Relay", "Connector"]),
    "Raw Materials": ("KG", (1.20, 28.0), ["Aluminium Billet", "Copper Rod", "Steel Sheet", "Polymer Granulate", "Glass Fibre"]),
    "Chemicals": ("L", (2.50, 65.0), ["Industrial Solvent", "Epoxy Resin", "Lubricant", "Coolant", "Adhesive"]),
    "Packaging": ("EA", (0.08, 4.50), ["Corrugated Box", "Pallet Wrap", "Foam Insert", "Shipping Crate", "Label Roll"]),
    "Mechanical Parts": ("EA", (6.0, 420.0), ["Bearing", "Gearbox", "Hydraulic Pump", "Valve Assembly", "Drive Shaft"]),
    "Fasteners": ("EA", (0.02, 1.80), ["Hex Bolt", "Lock Nut", "Rivet", "Washer", "Machine Screw"]),
}
CRITICALITY = {"CRITICAL": 10, "HIGH": 20, "MEDIUM": 40, "LOW": 30}
ALTERNATE_PROBABILITY = {"CRITICAL": 0.70, "HIGH": 0.50, "MEDIUM": 0.30, "LOW": 0.15}
WAREHOUSES = ["WH-ATH-01", "WH-THE-01", "WH-RTM-01", "WH-HAM-01", "WH-MIL-01"]
INVALID_CURRENCIES = ["EURO", "usd", "US$", "€", "XXX"]

FIELDS = [
    "product_id", "product_name", "category", "unit_of_measure", "unit_cost", "cost_currency",
    "criticality", "primary_supplier_id", "alternate_supplier_id", "stocking_location_id",
    "on_hand_qty", "safety_stock_qty", "avg_daily_demand",
]


def n_defects(n_rows, rate):
    return max(1, round(n_rows * rate))


def pick_rows(rng, rows, count, used):
    candidates = [i for i in range(len(rows)) if i not in used]
    chosen = rng.sample(candidates, min(count, len(candidates)))
    used.update(chosen)
    return chosen


def qty(value, uom):
    return f"{round(value):.2f}" if uom == "EA" else f"{value:.2f}"


def load_suppliers(path):
    suppliers = {}
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            suppliers.setdefault(row["supplier_id"], row)
    return suppliers


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


def generate(rng, suppliers):
    active = [s for s, r in suppliers.items() if r["is_active"] == "true"]
    all_ids = list(suppliers)
    categories = list(CATEGORIES)
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        category = rng.choice(categories)
        uom, (low, high), nouns = CATEGORIES[category]
        unit_cost = rng.uniform(low, high)
        criticality = rng.choices(list(CRITICALITY), list(CRITICALITY.values()))[0]
        primary = rng.choice(active) if rng.random() < 0.95 else rng.choice(all_ids)
        alternate = ""
        if rng.random() < ALTERNATE_PROBABILITY[criticality]:
            alternate = rng.choice([s for s in all_ids if s != primary])

        if unit_cost < 5:
            demand = rng.uniform(50, 600)
        elif unit_cost < 50:
            demand = rng.uniform(10, 150)
        else:
            demand = rng.uniform(1, 30)
        safety_stock = demand * rng.uniform(3, 10)
        on_hand = demand * rng.uniform(2, 60)

        rows.append({
            "product_id": f"PRD-{i:05d}",
            "product_name": f"{rng.choice(nouns)} {rng.choice('ABCSX')}{rng.randint(10, 99)}",
            "category": category,
            "unit_of_measure": uom,
            "unit_cost": f"{unit_cost:.4f}",
            "cost_currency": "EUR" if rng.random() < 0.85 else "USD",
            "criticality": criticality,
            "primary_supplier_id": primary,
            "alternate_supplier_id": alternate,
            "stocking_location_id": rng.choice(WAREHOUSES),
            "on_hand_qty": qty(on_hand, uom),
            "safety_stock_qty": qty(safety_stock, uom),
            "avg_daily_demand": f"{demand:.2f}",
        })
    return rows


def inject_defects(rng, rows):
    used = set()
    n = n_defects(len(rows), RULE_RATE)

    for i in pick_rows(rng, rows, n, used):
        column = rng.choice(["primary_supplier_id", "alternate_supplier_id"])
        rows[i][column] = f"SUP-{rng.randint(9001, 9999)}"
        rows[i]["_dq_rule"] = "unknown_supplier"
        rows[i]["_dq_description"] = f"{column} {rows[i][column]} does not exist in suppliers.csv"

    for i in pick_rows(rng, rows, n, used):
        column = rng.choice(["on_hand_qty", "safety_stock_qty"])
        rows[i][column] = f"-{rows[i][column]}" if float(rows[i][column]) > 0 else "-1.00"
        rows[i]["_dq_rule"] = "negative_quantity"
        rows[i]["_dq_description"] = f"{column} is negative ({rows[i][column]})"

    for i in pick_rows(rng, rows, n, used):
        original = rows[i]["cost_currency"]
        rows[i]["cost_currency"] = rng.choice(INVALID_CURRENCIES)
        rows[i]["_dq_rule"] = "currency_inconsistency"
        rows[i]["_dq_description"] = (
            f"cost_currency '{rows[i]['cost_currency']}' is not a valid ISO 4217 code (expected {original})"
        )

    inserts = {}
    for i in pick_rows(rng, rows, n, used):
        dup = {k: v for k, v in rows[i].items() if not k.startswith("_")}
        dup["unit_cost"] = f"{float(dup['unit_cost']) * rng.uniform(1.1, 1.4):.4f}"
        dup["_dq_rule"] = "duplicate_product"
        dup["_dq_description"] = f"Duplicate product_id {dup['product_id']} with conflicting unit_cost"
        inserts.setdefault(rng.randint(i, len(rows) - 1), []).append(dup)

    result = []
    for idx, row in enumerate(rows):
        result.append(row)
        result.extend(inserts.get(idx, []))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--seed", type=int, default=202)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    rng = random.Random(args.seed)
    suppliers = load_suppliers(out_dir / "suppliers.csv")

    rows = inject_defects(rng, generate(rng, suppliers))
    errors = write_outputs(rows, out_dir, "products")
    print(f"products.csv: {len(rows)} rows | products_errors.csv: {len(errors)} rows")


if __name__ == "__main__":
    main()
