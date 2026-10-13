"""Executable API examples (assignment 6.7): one call per endpoint, each with the result it must return.

Start the API first, then in a second terminal:
    cd src/ilias_api_code
    uv run python api_examples.py
The API needs a Microsoft Entra ID access token for it: put the token in API_TOKEN (environment or .env),
for example with the Azure CLI: az account get-access-token --scope api://<client-id>/access_as_user
Set API_BASE_URL to call a server other than http://127.0.0.1:8000."""
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.environ.get('API_BASE_URL', 'http://127.0.0.1:8000')
TOKEN = os.environ.get('API_TOKEN', '').strip()
AUTH = {'Authorization': f'Bearer {TOKEN}'} if TOKEN else {}


def has_results(body):
    return isinstance(body.get('results'), list) and len(body['results']) > 0


EXAMPLES = [
    # (name, method, path, request options, expected status, check on the JSON answer)
    ('Liveness', 'GET', '/health', {}, 200, lambda b: b['status'] == 'ok'),
    ('Readiness (Databricks answers)', 'GET', '/ready', {}, 200, lambda b: b['status'] == 'ready'),

    ('KPI: Order Value (total)', 'POST', '/analytics/query',
     {'json': {'kpi': 'order_value'}}, 200, lambda b: has_results(b) and b['unit'] == 'EUR'),
    ('KPI: On-Time Delivery Rate by region', 'POST', '/analytics/query',
     {'json': {'kpi': 'on_time_delivery_rate', 'group_by': 'region'}}, 200,
     lambda b: has_results(b) and all(0 <= r['value'] <= 1 for r in b['results'])),
    ('KPI: Order Value by month (EMEA, 2026)', 'POST', '/analytics/query',
     {'json': {'kpi': 'order_value', 'group_by': 'month', 'region': 'EMEA',
               'order_date_from': '2026-01-01', 'order_date_to': '2026-12-31'}}, 200, has_results),
    ('KPI: Exposed Value of CRITICAL products', 'POST', '/analytics/query',
     {'json': {'kpi': 'exposed_value', 'criticality': 'CRITICAL'}}, 200, has_results),
    ('Error: unknown KPI', 'POST', '/analytics/query', {'json': {'kpi': 'profit_margin'}}, 422, None),
    ('Error: month trend on a snapshot KPI', 'POST', '/analytics/query',
     {'json': {'kpi': 'exposed_value', 'group_by': 'month'}}, 422, None),
    ('Error: missing sign-in token', 'POST', '/analytics/query',
     {'json': {'kpi': 'order_value'}, 'headers': {'Authorization': ''}}, 401, None),

    ('Supplier scorecard (worst on-time first)', 'GET', '/suppliers/scorecard',
     {'params': {'sort_by': 'on_time_delivery_rate', 'limit': 5}}, 200, lambda b: len(b['suppliers']) <= 5),
    ('Product exposure (CRITICAL that run out)', 'GET', '/products/exposure',
     {'params': {'criticality': 'CRITICAL', 'limit': 10}}, 200,
     lambda b: all(p['criticality'] == 'CRITICAL' and p['is_exposed'] for p in b['products'])),
    ('Delayed order lines (mitigation queue)', 'GET', '/orders/delayed',
     {'params': {'only_exposed_products': 'true', 'limit': 10}}, 200, lambda b: isinstance(b['order_lines'], list)),
    ('Error: limit out of range', 'GET', '/suppliers/scorecard', {'params': {'limit': 1000}}, 422, None),

    ('Assistant: KPI question (needs Azure OpenAI)', 'POST', '/assistant/query',
     {'json': {'question': 'What is our on-time delivery rate?'}}, 200,
     lambda b: b['status'] == 'answered' and len(b['evidence']) > 0),
    ('Assistant: out-of-scope request', 'POST', '/assistant/query',
     {'json': {'question': 'Cancel purchase order PO-2026-00437 and email the supplier.'}}, 200,
     lambda b: b['status'] == 'out_of_scope'),
]


def main():
    failed = 0
    with httpx.Client(base_url=BASE_URL, headers=AUTH, timeout=180) as client:
        for name, method, path, options, expected, check in EXAMPLES:
            try:
                r = client.request(method, path, **options)
            except httpx.ConnectError:
                sys.exit(f'Cannot reach the API at {BASE_URL}. Is uvicorn running?')
            try:
                ok = (r.status_code == expected and 'X-Correlation-ID' in r.headers
                      and (check is None or check(r.json())))
            except (KeyError, TypeError, ValueError):
                ok = False
            failed += not ok
            print(f"{'PASS' if ok else 'FAIL'}  {r.status_code}  {method} {path}  - {name}")
            print(f'      {r.text[:150]}')
    print(f'\n{len(EXAMPLES) - failed}/{len(EXAMPLES)} examples passed')
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()

