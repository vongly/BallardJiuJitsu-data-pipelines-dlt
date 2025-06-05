import dlt
from dlt.sources import incremental
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

import sys
from pathlib import Path
import time

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))

from env import STRIPE_API_SECRET

BASE_URL = 'https://api.stripe.com/v1'

client = RESTClient(
    base_url=BASE_URL,
    auth=BearerTokenAuth(token=STRIPE_API_SECRET),
)

# Loads records from stripe API endpoint -> incremental value keys off of 'id'
def query_stripe_incremental_created(data_source, incremental_obj=None):
    start = time.time()
    print(f'  Processing - { data_source }')

    params = {'limit': 100}

    if incremental_obj:
        if incremental_obj.last_value:
            params["created[gte]"] = incremental_obj.last_value

    count = 0
    while True:
        response = client.get(data_source, params=params)
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
    print(f'  Record Count: { data_source } -', count, f'({end - start:.1f}s)')

def create_stripe_resource_incremental_created(**kwargs):
    data_source = kwargs['data_source']
    resource_name = f'stripe_{ data_source }_incremental_created'
    @dlt.resource(name=resource_name, write_disposition='append', primary_key="id")
    def created_resource(incremental_obj=incremental('created', initial_value=None)):
        yield from query_stripe_incremental_created(data_source, incremental_obj)
    return created_resource()