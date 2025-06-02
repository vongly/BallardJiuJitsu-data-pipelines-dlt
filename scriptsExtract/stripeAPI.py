import dlt
from dlt.sources import incremental
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

import sys
from pathlib import Path
import time

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import STRIPE_API_SECRET

BASE_URL = 'https://api.stripe.com/v1'

client = RESTClient(
    base_url=BASE_URL,
    auth=BearerTokenAuth(token=STRIPE_API_SECRET),
)

STRIPE_ENDPOINTS_INCREMENTAL_ID = [
    'refunds',
    'customers',
    'charges',
    'invoices',
]

# Loads records from stripe API endpoint -> incremental value keys off of 'id'
def query_stripe_endpoint_incremental_id(endpoint, incremental_id=None, params=None):
    start = time.time()
    print(f'  Processing - { endpoint }')
    params = params={'limit': 100} if params == None else params
    if incremental_id:
        if incremental_id.last_value:
            params['starting_after'] = incremental_id.last_value
    count = 0
    while True:
        response = client.get(endpoint, params=params)
        if response.status_code != 200:
            print(f' Request failed: { response.status_code }, { response.text }')

        json_data = response.json()
        data = json_data.get('data', [])

        if not data:
            break
        for record in data:
            count += 1
            yield record
        if not json_data.get('has_more'):
            break

        params['starting_after'] = data[-1]['id']
        time.sleep(0.1)

    end = time.time()
    print(f'  Record Count: { endpoint } -', count, f'({end - start:.1f}s)')

def create_stripe_resource_incremental_id(endpoint):
    resource_name = f'stripe_{ endpoint }_incremental_id'
    @dlt.resource(name=resource_name, write_disposition='append')
    def created_resource(incremental_id=incremental('id', initial_value=None)):
        yield from query_stripe_endpoint_incremental_id(endpoint, incremental_id)
    return created_resource

if __name__ == '__main__':
    records = query_stripe_endpoint_incremental_id(
        endpoint = 'refunds',
        incremental_id=None,
    )

    for r in records:
        print(r)