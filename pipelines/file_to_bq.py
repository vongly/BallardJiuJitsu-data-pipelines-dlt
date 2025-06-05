import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline_file_to_destination import create_file_to_destination_pipeline
from utils.scriptsLoad.from_file import create_file_resource
from env import BQ_SERVICE_ACCOUNT_JSON_PATH

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_SERVICE_ACCOUNT_JSON_PATH


def run_file_to_bq_pipeline():

    DATASET_TABLES_SQLITE = {
        'pipeline': 'sqlite_to_file',
        'dataset': 'sqlite',
        'tables': [
            {'table': 'sqlite_users_incremental_updated_at'},
            {'table': 'sqlite_kids_incremental_updated_at'},
            {'table': 'sqlite_class_times_incremental_updated_at'},
            {'table': 'sqlite_class_time_checkins_incremental_updated_at'},
            {'table': 'sqlite_kids_class_time_checkins_incremental_updated_at'},
        ]
    }
    DATASET_TABLES_STRIPE = {
        'pipeline': 'stripe_to_file',
        'dataset': 'stripe',
        'tables': [
            {'table': 'stripe_charges_incremental_created'},
            {'table': 'stripe_customers_incremental_created'},
            {'table': 'stripe_invoices_incremental_created'},
            {'table': 'stripe_refunds_incremental_created'},
        ]
    }

    datasets_tables_files_to_bq = [DATASET_TABLES_SQLITE, DATASET_TABLES_STRIPE]

    for dataset_tables in datasets_tables_files_to_bq:
        dataset = dataset_tables['dataset']
        name = f'{ dataset }_file_to_bq'
        
        create_file_to_destination_pipeline(
            name=name,
            destination='bigquery',
            create_resource_function=create_file_resource,
            dataset_tables=dataset_tables,
        )

if __name__ == '__main__':
    run_file_to_bq_pipeline()