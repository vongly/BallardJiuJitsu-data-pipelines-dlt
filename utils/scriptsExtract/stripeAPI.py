import dlt
from dlt.sources import incremental
import requests

import sys
from pathlib import Path
import time

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))

from env import STRIPE_API_SECRET

BASE_URL = 'https://api.stripe.com/v1'

# Loads records from stripe API endpoint -> incremental value keys off of 'created'
def query_stripe_incremental_created(resource_name, data_source, incremental_obj=None):
    start = time.time()
    print(f'  Processing - { resource_name }')

    headers = {
        'Authorization': f'Bearer { STRIPE_API_SECRET }'
    }

    params = {'limit': 100}

    if incremental_obj:
        if incremental_obj.last_value:
            params['created[gte]'] = incremental_obj.last_value

    count = 0
    while True:
        response = requests.get(data_source, headers=headers, params=params)
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
    print(f'  Record Count - { resource_name }:', count, f'({end - start:.1f}s)')

def create_stripe_resource_incremental_created(resource_details):
    pipeline_name = resource_details['pipeline_name']
    data_source = resource_details['data_source']

    table_name = f'stripe_{ data_source }__incremental_created'
    resource_name = f'{ pipeline_name }__{ table_name }'

    @dlt.resource(name=resource_name, table_name=table_name, write_disposition='append', primary_key='id')
    def created_resource(incremental_obj=incremental('created', initial_value=None)):
        yield from query_stripe_incremental_created(resource_name, data_source, incremental_obj)
    return created_resource()