import dlt

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from scriptsExtract.stripeAPI import (
    create_stripe_resource_incremental_id,
    STRIPE_ENDPOINTS_INCREMENTAL_ID,
)
from env import PIPELINES_DIR
from utils import print_pipeline_details

os.environ['DLT_SECRETS_FILE'] = os.path.join(os.path.dirname(__file__), 'secrets.toml')
os.environ['DLT_CONFIG_FILE'] = os.path.join(os.path.dirname(__file__), 'config.toml')


def pipeline(stripe_endpoints_incremental_id):

    pipeline_object = dlt.pipeline(
        pipeline_name='stripe_to_file',
        destination='filesystem',
        dataset_name='stripe',
        pipelines_dir=PIPELINES_DIR,
    )

    print_pipeline_details(pipeline_object)

    @dlt.source
    def source_object(stripe_endpoints_incremental_id=None):
        if stripe_endpoints_incremental_id == None:
            stripe_endpoints_incremental_id = pipeline_object.stripe_endpoints_incremental_id
        for endpoint in stripe_endpoints_incremental_id:
            yield create_stripe_resource_incremental_id(endpoint=endpoint)
        print('\n')

    pipeline_object.stripe_endpoints_incremental_id = stripe_endpoints_incremental_id

    load_info = pipeline_object.run(source_object())
    print('\n', load_info)

    return load_info


if __name__ == '__main__':
    pipeline(stripe_endpoints_incremental_id=STRIPE_ENDPOINTS_INCREMENTAL_ID)