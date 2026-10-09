"""Business endpoints: exact brief KPI values on the generated data, filters, safety and contract."""
import pytest

H = {'X-API-Key': 'secret'}
EXPECTED = {'order_value': 190388429.60, 'on_time_delivery_rate': 0.6446, 'average_delay_days': 6.74,
            'supplier_fill_rate': 0.8426, 'exposed_value': 2574955.37, 'delayed_quantity': 6929712.71,
            'critical_item_count': 22, 'alternate_supplier_coverage': 0.5862}


def test_health(client):
    assert client.get('/health').json() == {'status': 'ok'}


@pytest.mark.parametrize('kpi, value', EXPECTED.items())
def test_brief_kpis(client, kpi, value):
    r = client.post('/analytics/query', json={'kpi': kpi}, headers=H).json()
    assert abs(r['results'][0]['value'] - value) < 0.011 and r['as_of_date'] == '2026-10-05'


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
    assert exposed['count'] == 36
    assert abs(sum(p['exposed_value_eur'] for p in exposed['products']) - EXPECTED['exposed_value']) < 1
    no_backup = client.get('/products/exposure', params={'only_exposed': False, 'criticality': 'CRITICAL',
                                                         'without_active_alternate': True, 'limit': 300},
                           headers=H).json()
    assert no_backup['count'] == 12
    queue = client.get('/orders/delayed', params={'limit': 500, 'only_exposed_products': True}, headers=H).json()
    assert queue['count'] == 72 and all(x['product_is_exposed'] for x in queue['order_lines'])


def test_operation_ids_for_agents(client):
    ids = {op['operationId'] for path in client.get('/openapi.json').json()['paths'].values() for op in path.values()}
    assert ids == {'get_health', 'query_kpi', 'get_supplier_scorecard', 'get_product_exposure',
                   'get_delayed_order_lines', 'ask_assistant'}


def test_user_interface_is_served_without_data(client):
    page = client.get('/')
    assert page.status_code == 200 and page.headers['content-type'].startswith('text/html')
    assert 'Supply disruption desk' in page.text and 'X-API-Key' in page.text   # data still needs the key
