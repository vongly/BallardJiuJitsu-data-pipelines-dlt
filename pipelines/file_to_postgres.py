import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core import create_pipeline
from utils.resources.from_file import FileResource
from env import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_CERTIFICATION_PATH,
    EXTRACT_DIR,
)

os.environ['DESTINATION__POSTGRES__CREDENTIALS__HOST'] = POSTGRES_HOST
os.environ['DESTINATION__POSTGRES__CREDENTIALS__PORT'] = POSTGRES_PORT
os.environ['DESTINATION__POSTGRES__CREDENTIALS__USERNAME'] = POSTGRES_USER
os.environ['DESTINATION__POSTGRES__CREDENTIALS__PASSWORD'] = POSTGRES_PASSWORD
os.environ['DESTINATION__POSTGRES__CREDENTIALS__DATABASE'] = POSTGRES_DB
os.environ['DESTINATION__POSTGRES__CREDENTIALS__SSLMODE'] = 'verify-full'
os.environ['DESTINATION__POSTGRES__CREDENTIALS__SSLROOTCERT'] = POSTGRES_CERTIFICATION_PATH

def run_stripe_file_to_bq_pipeline():

    data_sources_stripe = [
        'charges__incr_created',
        'customers__incr_created',
        'invoices__incr_created',
        'invoices__incr_created__lines__data',
        'invoices__incr_created__lines__data__discount_amounts',
        'refunds__incr_created',
    ]

    extract_pipeline_name='stripe_to_file'
    pipeline_name = 'stripe_file_to_postgres'
    destination = 'postgres'
    dataset = 'stripe'

    resources = [
        FileResource(
            extract_dir=EXTRACT_DIR,
            file_pipeline_name=extract_pipeline_name,
            file_dataset=dataset,
            file_data_source=data_source,
        ).create_resource()

            for data_source in data_sources_stripe
    ]

    if resources:
        pipeline = create_pipeline.Pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset=dataset,
            resources=resources,
        )

        pipeline.run_pipeline()
        return pipeline

def run_sqlite_file_to_bq_pipeline():

    data_sources_sqlite = [
        'users__incr_updated_at',
        'kids__incr_updated_at',
        'class_times__incr_updated_at',
        'class_time_checkins__incr_updated_at',
        'kids_class_time_checkins__incr_updated_at',
    ]

    extract_pipeline_name='sqlite_to_file'
    pipeline_name = 'sqlite_file_to_postgres'
    destination = 'postgres'
    dataset = 'sqlite'
    
    resources = [
        FileResource(
            extract_dir=EXTRACT_DIR,
            file_pipeline_name=extract_pipeline_name,
            file_dataset=dataset,
            file_data_source=data_source,
        ).create_resource()

            for data_source in data_sources_sqlite
    ]

    if resources:
        pipeline = create_pipeline.Pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset=dataset,
            resources=resources,
        )

        pipeline.run_pipeline()
        return pipeline

def run_file_to_postgress_pipeline():
    pipeline_stripe = run_stripe_file_to_bq_pipeline()
    print(pipeline_stripe.jobs_json)

    pipeline_sqlite = run_sqlite_file_to_bq_pipeline()
    print(pipeline_sqlite.jobs_json)

    return {
        'stripe': pipeline_stripe,
        'sqlite': pipeline_sqlite,
    }
if __name__ == '__main__':
    pipeline = run_file_to_postgress_pipeline()

#    pipeline_sqlite = run_sqlite_file_to_bq_pipeline()
#    print(pipeline_sqlite.jobs_json)
