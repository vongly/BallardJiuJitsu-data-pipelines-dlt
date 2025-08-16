import dlt

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core import create_pipeline
from utils.resources.stripeAPI import StripeResource
from env import EXTRACT_DIR


def run_stripe_to_file_pipeline():

    data_sources_stripe = [
        'refunds',
        'customers',
        'charges',
        'invoices',
    ]

    resources = [
        StripeResource(
            data_source=data_source,
            incremental_attribute='created',
            table_name_suffix='__incr_created',
        ).create_resource()

            for data_source in data_sources_stripe
    ]

    pipeline_name = 'stripe_to_file'
    destination = f'{ EXTRACT_DIR }/{ pipeline_name }'
    dataset = 'stripe'

    pipeline = create_pipeline.Pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.filesystem(bucket_url=destination),
        dataset=dataset,
        resources=resources,
    )

    pipeline.run_pipeline()
    return pipeline

if __name__ == '__main__':
    pipeline = run_stripe_to_file_pipeline()
    print(pipeline.jobs_json)