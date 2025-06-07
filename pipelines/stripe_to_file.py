import dlt

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline_data_source_to_file import CreateDataSourceToFilePipeline
from utils.scriptsExtract.stripeAPI import create_stripe_resource_incremental_created
from env import EXTRACT_DIR

def run_stripe_to_file_pipeline():

    STRIPE_DATA_SOURCES_INCREMENTAL_CREATED = [
        'refunds',
        'customers',
        'charges',
        'invoices',
    ]

    pipeline_name='stripe_to_file'
    destination=f'{ EXTRACT_DIR }/{ pipeline_name }'
    dataset='stripe'

    pipeline = CreateDataSourceToFilePipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset=dataset,
        create_resource_function=create_stripe_resource_incremental_created,
        data_sources=STRIPE_DATA_SOURCES_INCREMENTAL_CREATED,
    )

    pipeline.run_all()

    return pipeline

if __name__ == '__main__':
    run_stripe_to_file_pipeline()