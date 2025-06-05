import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline_file_to_destination import CreateFileToDistinationPipeline
from utils.scriptsLoad.from_file import create_file_resource
from env import BQ_SERVICE_ACCOUNT_JSON_PATH

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_SERVICE_ACCOUNT_JSON_PATH

def run_stripe_file_to_bq_pipeline():

    DATA_SOURCES_STRIPE = [
        'stripe_charges_incremental_created',
        'stripe_customers_incremental_created',
        'stripe_invoices_incremental_created',
        'stripe_refunds_incremental_created',
    ]


    pipeline_name = 'stripe_file_to_bq'
    destination = 'bigquery'
    dataset = 'stripe'
    extract_pipeline_name='stripe_to_file'

    pipeline = CreateFileToDistinationPipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset=dataset,
        create_resource_function=create_file_resource,
        extract_pipeline_name=extract_pipeline_name,
        data_sources=DATA_SOURCES_STRIPE,
    )

    pipeline.run_all()

def run_sqlite_file_to_bq_pipeline():

    DATA_SOURCES_SQLITE = [
        'sqlite_users_incremental_updated_at',
        'sqlite_kids_incremental_updated_at',
        'sqlite_class_times_incremental_updated_at',
        'sqlite_class_time_checkins_incremental_updated_at',
        'sqlite_kids_class_time_checkins_incremental_updated_at',
    ]

    pipeline_name = 'sqlite_file_to_bq'
    destination = 'bigquery'
    dataset = 'sqlite'
    extract_pipeline_name='sqlite_to_file'
    
    pipeline = CreateFileToDistinationPipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset=dataset,
        create_resource_function=create_file_resource,
        extract_pipeline_name=extract_pipeline_name,
        data_sources=DATA_SOURCES_SQLITE,
    )

    pipeline.run_all()


if __name__ == '__main__':
    run_stripe_file_to_bq_pipeline()
    run_sqlite_file_to_bq_pipeline()