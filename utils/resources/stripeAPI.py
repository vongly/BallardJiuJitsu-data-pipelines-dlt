import dlt
from dlt.sources import incremental
import requests

import sys
from pathlib import Path
import time
from datetime import datetime, timezone

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))

from env import STRIPE_API_SECRET

BASE_URL = 'https://api.stripe.com/v1'

class StripeResource:
    def __init__(
            self,
            data_source,
            incremental_attribute=None,
            **kwargs
    ):

        self.data_source = data_source
        self.incremental_obj = incremental(incremental_attribute, None)

        # **kwargs
        self.table_name_suffix = kwargs.get('table_name_suffix', '')

    # Loads records from stripe API endpoint -> incremental value keys off of 'created'
    def yield_query_results(self, incremental_obj=None):
        url = f'{ BASE_URL }/{ self.data_source }'
        headers = {
            'Authorization': f'Bearer { STRIPE_API_SECRET }'
        }

        params = {'limit': 100}

        if incremental_obj:
            if incremental_obj.last_value:
                params['created[gte]'] = incremental_obj.last_value

        while True:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f' Request failed: { response.status_code }, { response.text }')

            json_data = response.json()
            data = json_data.get('data', [])

            if not data:
                break
            for record in data:
                record['_dlt_processed'] = datetime.now(timezone.utc)
                yield record
            if not json_data.get('has_more'):
                break

            params['starting_after'] = data[-1]['id']
            time.sleep(0.1)

    def create_resource(self):
        table_name = self.data_source + self.table_name_suffix
        @dlt.resource(name=self.data_source, table_name=table_name, write_disposition='append', primary_key='id')
        def my_resource(incremental_obj=self.incremental_obj):
            yield from self.yield_query_results(incremental_obj)
        return my_resource()