"""Business endpoints: exact brief KPI values on the generated data, filters, safety and contract."""
import time

import pytest
from conftest import CLIENT, TENANT, H, bearer, token
EXPECTED = {'order_value': 190388429.60, 'on_time_delivery_rate': 0.6446, 'average_delay_days': 6.74,
            'supplier_fill_rate': 0.8379, 'exposed_value': 2461405.16, 'delayed_quantity': 6997542.24,
            'critical_item_count': 22, 'alternate_supplier_coverage': 0.5862}


def test_health(client):
    assert client.get('/health').json() == {'status': 'ok'}


@pytest.mark.parametrize('kpi, value', EXPECTED.items())
def test_brief_kpis(client, kpi, value):
    r = client.post('/analytics/query', json={'kpi': kpi}, headers=H).json()
    assert abs(r['results'][0]['value'] - value) < 0.011 and r['as_of_date'] == '2026-10-07'


def test_groupings_reconcile(client):
    by_month = client.post('/analytics/query', json={'kpi': 'on_time_delivery_rate', 'group_by': 'month'},
                           headers=H).json()['results']
    by_region = client.post('/analytics/query', json={'kpi': 'order_value', 'group_by': 'region'},
                            headers=H).json()['results']
    assert len(by_month) == 12 and abs(sum(x['value'] for x in by_region) - EXPECTED['order_value']) < 1


def test_validation_and_safety(client):
    assert client.post('/analytics/query', json={'kpi': 'order_value'}).status_code == 401
    assert client.post('/analytics/query', json={'kpi': 'exposed_value', 'group_by': 'month'},
                       headers=H).status_code == 422
    assert client.post('/analytics/query', json={'kpi': 'order_value', 'supplier_id': "x' OR '1'='1"},
                       headers=H).status_code == 422
    injected = client.post('/analytics/query', json={'kpi': 'order_value', 'category': "x' OR '1'='1"},
                           headers=H).json()
    assert injected['results'][0]['value'] is None          # bound as a parameter: matches nothing


def test_scorecard_exposure_and_queue(client):
    scorecard = client.get('/suppliers/scorecard', headers=H).json()
    rates = [s['on_time_delivery_rate'] for s in scorecard['suppliers']]
    assert rates == sorted(rates) and scorecard['count'] == 10
    exposed = client.get('/products/exposure', params={'limit': 300}, headers=H).json()
    assert exposed['count'] == 28
    assert abs(sum(p['exposed_value_eur'] for p in exposed['products']) - EXPECTED['exposed_value']) < 1
    no_backup = client.get('/products/exposure', params={'only_exposed': False, 'criticality': 'CRITICAL',
                                                         'without_active_alternate': True, 'limit': 300},
                           headers=H).json()
    assert no_backup['count'] == 12
    queue = client.get('/orders/delayed', params={'limit': 500, 'only_exposed_products': True}, headers=H).json()
    assert queue['count'] == 61 and all(x['product_is_exposed'] for x in queue['order_lines'])


def test_operation_ids_for_agents(client):
    ids = {op['operationId'] for path in client.get('/openapi.json').json()['paths'].values() for op in path.values()}
    assert ids == {'get_health', 'get_readiness', 'query_kpi', 'get_supplier_scorecard', 'get_product_exposure',
                   'get_delayed_order_lines', 'get_supplier_disruption_impact', 'ask_assistant'}


def test_user_interface_is_served_without_data(client):
    page = client.get('/')
    assert page.status_code == 200 and page.headers['content-type'].startswith('text/html')
    assert 'Supply disruption desk' in page.text and 'Sign in with Microsoft' in page.text   # data needs sign-in
    assert client.get('/auth/config').json() == {
        'client_id': CLIENT, 'authority': f'https://login.microsoftonline.com/{TENANT}',
        'scope': f'api://{CLIENT}/access_as_user'}


@pytest.mark.parametrize('headers', [
    {},
    {'Authorization': 'Bearer not-a-jwt'},
    bearer(aud='api://some-other-api'),
    bearer(iss='https://login.microsoftonline.com/other-tenant/v2.0'),
    bearer(tid='33333333-3333-3333-3333-333333333333'),
    bearer(exp=int(time.time()) - 60),
    bearer(tid=None),
])
def test_rejected_tokens(client, headers):
    r = client.post('/analytics/query', json={'kpi': 'order_value'}, headers=headers)
    assert r.status_code == 401 and r.headers['www-authenticate'] == 'Bearer'


def test_token_permissions(client):
    ok = lambda headers: client.get('/suppliers/scorecard', params={'limit': 1}, headers=headers).status_code
    v1_token = {'Authorization': f'Bearer {token(aud=f"api://{CLIENT}", iss=f"https://sts.windows.net/{TENANT}/")}'}
    assert ok(v1_token) == 200
    assert ok(bearer(scp='User.Read')) == 403
    assert ok(bearer(scp=None, roles=['SupplyChain.Read'])) == 200        # app-only caller with the app role
    assert ok(bearer(scp=None, roles=['Something.Else'])) == 403
