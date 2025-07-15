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
        'stripe_charges__incremental_created',
        'stripe_customers__incremental_created',
        'stripe_invoices__incremental_created',
        'stripe_invoices__incremental_created__lines__data',
        'stripe_invoices__incremental_created__lines__data__discount_amounts',
        'stripe_refunds__incremental_created',
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
    return pipeline

def run_sqlite_file_to_bq_pipeline():

    DATA_SOURCES_SQLITE = [
        'sqlite_users__incremental_updated_at',
        'sqlite_kids__incremental_updated_at',
        'sqlite_class_times__incremental_updated_at',
        'sqlite_class_time_checkins__incremental_updated_at',
        'sqlite_kids_class_time_checkins__incremental_updated_at',
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
    return pipeline

def run_file_to_bq_pipeline():
    pipeline_stripe = run_stripe_file_to_bq_pipeline()
    print(pipeline_stripe.jobs_json)

    pipeline_sqlite = run_sqlite_file_to_bq_pipeline()
    print(pipeline_sqlite.jobs_json)

    return {
        'stripe': pipeline_stripe,
        'sqlite': pipeline_sqlite,
    }
if __name__ == '__main__':
    pipeline = run_file_to_bq_pipeline()