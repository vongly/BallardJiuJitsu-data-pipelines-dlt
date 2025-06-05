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

#BASE_URL = 'https://usawfl-classification-calculator.vong-ly.com/api'

client = RESTClient(
    base_url=BASE_URL,
#    auth=BearerTokenAuth(token=TOKEN),
)

# Loads records from stripe API endpoint -> incremental value keys off of 'id'
def query_api(data_source):
    start = time.time()
    print(f'  Processing - { data_source }')

    params = {}

    count = 0

    response = client.get(data_source, params=params)
    if response.status_code != 200:
        print(f' Request failed: { response.status_code }, { response.text }')

    json_data = response.json()
    data = json_data

    for record in data:
        count += 1
        yield record

        params['starting_after'] = data[-1]['id']
        time.sleep(0.1)

    end = time.time()
    print(f'  Record Count: { data_source } -', count, f'({end - start:.1f}s)')

def create_api_resorce(**kwargs):
    data_source = kwargs['data_source']
    resource_name = data_source
    @dlt.resource(name=resource_name, write_disposition='append',)
    def created_resource():
        yield from query_api(data_source)
    return created_resource()

