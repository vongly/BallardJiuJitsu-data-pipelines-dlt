import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline_data_source_to_file import create_data_source_to_file_pipeline
from utils.scriptsExtract.stripeAPI import create_stripe_resource_incremental_created


def run_stripe_to_file_pipeline():

    STRIPE_DATA_SOURCES_INCREMENTAL_CREATED = [
        'refunds',
        'customers',
        'charges',
        'invoices',
    ]

    stripe_resource_details = [ {'data_source': data_source} for data_source in STRIPE_DATA_SOURCES_INCREMENTAL_CREATED ]

    create_data_source_to_file_pipeline(
        name='stripe_to_file',
        destination='filesystem',
        dataset='stripe',
        create_resource_function=create_stripe_resource_incremental_created,
        resources_details=stripe_resource_details,
    )

if __name__ == '__main__':
    run_stripe_to_file_pipeline()