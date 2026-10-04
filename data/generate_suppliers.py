"""Generate suppliers.csv and suppliers_errors.csv.

Run order: 1) generate_suppliers.py  2) generate_products.py
           3) generate_purchase_orders.py  4) generate_shipment_events.py
"""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

N_SUPPLIERS = 50
RULE_RATE = 0.02  # share of rows per data-quality rule (minimum 1 row)

# country: (region, currency)
COUNTRIES = {
    "DE": ("EMEA", "EUR"), "FR": ("EMEA", "EUR"), "IT": ("EMEA", "EUR"),
    "NL": ("EMEA", "EUR"), "ES": ("EMEA", "EUR"), "GR": ("EMEA", "EUR"),
    "PL": ("EMEA", "PLN"), "GB": ("EMEA", "GBP"), "CH": ("EMEA", "CHF"),
    "CN": ("APAC", "CNY"), "JP": ("APAC", "JPY"), "IN": ("APAC", "INR"),
    "KR": ("APAC", "KRW"), "TW": ("APAC", "USD"), "VN": ("APAC", "USD"),
    "US": ("AMER", "USD"), "MX": ("AMER", "MXN"),
}
COUNTRY_WEIGHTS = {
    "DE": 6, "FR": 3, "IT": 4, "NL": 3, "ES": 2, "GR": 3, "PL": 3, "GB": 3, "CH": 2,
    "CN": 8, "JP": 2, "IN": 4, "KR": 2, "TW": 2, "VN": 2, "US": 4, "MX": 2,
}
LEGAL_FORM = {
    "DE": "GmbH", "FR": "SAS", "IT": "S.p.A.", "NL": "B.V.", "ES": "S.L.", "GR": "S.A.",
    "PL": "Sp. z o.o.", "GB": "Ltd", "CH": "AG", "CN": "Co., Ltd.", "JP": "K.K.",
    "IN": "Pvt Ltd", "KR": "Co., Ltd.", "TW": "Co., Ltd.", "VN": "JSC", "US": "Inc.",
    "MX": "S.A. de C.V.",
}
LEAD_TIME_DAYS = {"EMEA": (7, 21), "AMER": (21, 40), "APAC": (30, 55)}
NAME_PREFIX = [
    "Apex", "Nordic", "Blue River", "Delta", "Summit", "Atlas", "Vertex", "Pioneer",
    "Orion", "Meridian", "Silverline", "Crescent", "Evergreen", "Horizon", "Ironwood",
    "Keystone", "Lumen", "Northstar", "Pacific", "Granite",
]
NAME_CORE = [
    "Components", "Industrial", "Materials", "Electronics", "Chemicals", "Packaging",
    "Metals", "Manufacturing", "Supply", "Precision",
]
INVALID_CURRENCIES = ["EURO", "usd", "US$", "€", "XXX"]

FIELDS = [
    "supplier_id", "supplier_name", "country_code", "region", "default_currency",
    "contract_lead_time_days", "risk_rating", "is_active", "onboarded_date",
]


def n_defects(n_rows, rate):
    return max(1, round(n_rows * rate))


def pick_rows(rng, rows, count, used):
    candidates = [i for i in range(len(rows)) if i not in used]
    chosen = rng.sample(candidates, min(count, len(candidates)))
    used.update(chosen)
    return chosen


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


def generate(rng):
    rows, names = [], set()
    countries, weights = zip(*COUNTRY_WEIGHTS.items())
    for i in range(1, N_SUPPLIERS + 1):
        country = rng.choices(countries, weights)[0]
        region, currency = COUNTRIES[country]
        while True:
            name = f"{rng.choice(NAME_PREFIX)} {rng.choice(NAME_CORE)} {LEGAL_FORM[country]}"
            if name not in names:
                names.add(name)
                break
        rows.append({
            "supplier_id": f"SUP-{i:04d}",
            "supplier_name": name,
            "country_code": country,
            "region": region,
            "default_currency": currency,
            "contract_lead_time_days": rng.randint(*LEAD_TIME_DAYS[region]),
            "risk_rating": rng.choices(["LOW", "MEDIUM", "HIGH"], [50, 35, 15])[0],
            "is_active": "true" if rng.random() < 0.92 else "false",
            "onboarded_date": (date(2015, 1, 1) + timedelta(days=rng.randint(0, 3833))).isoformat(),
        })
    return rows


def inject_defects(rng, rows):
    used = set()
    n = n_defects(len(rows), RULE_RATE)

    for i in pick_rows(rng, rows, n, used):
        original = rows[i]["default_currency"]
        rows[i]["default_currency"] = rng.choice(INVALID_CURRENCIES)
        rows[i]["_dq_rule"] = "currency_inconsistency"
        rows[i]["_dq_description"] = (
            f"default_currency '{rows[i]['default_currency']}' is not a valid ISO 4217 code "
            f"(expected {original})"
        )

    inserts = {}
    for i in pick_rows(rng, rows, n, used):
        dup = {k: v for k, v in rows[i].items() if not k.startswith("_")}
        dup["contract_lead_time_days"] = int(dup["contract_lead_time_days"]) + rng.randint(3, 15)
        dup["risk_rating"] = rng.choice([r for r in ["LOW", "MEDIUM", "HIGH"] if r != dup["risk_rating"]])
        dup["_dq_rule"] = "duplicate_supplier"
        dup["_dq_description"] = f"Duplicate supplier_id {dup['supplier_id']} with conflicting attributes"
        inserts.setdefault(rng.randint(i, len(rows) - 1), []).append(dup)

    result = []
    for idx, row in enumerate(rows):
        result.append(row)
        result.extend(inserts.get(idx, []))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--seed", type=int, default=101)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)

    rows = inject_defects(rng, generate(rng))
    errors = write_outputs(rows, out_dir, "suppliers")
    print(f"suppliers.csv: {len(rows)} rows | suppliers_errors.csv: {len(errors)} rows")


if __name__ == "__main__":
    main()
