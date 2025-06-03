from utils.helpers import pretty_all_jsons

from pipelines.data_source_to_file import create_pipeline

from scriptsExtract.stripeAPI import (
    create_stripe_resource_incremental_created,
    STRIPE_DATA_SOURCES_INCREMENTAL_CREATED,
)

from scriptsExtract.sqliteDB import (
    copy_sqlite_db,
    create_sqliteDB_resource_incremental_updated_at,
    SQLITE_DATA_SOURCES_INCREMENTAL_UPDATED_AT,
)

from pipelines.file_to_bq import (
    pipeline as file_to_bq_pipeline, DATASET_TABLES_STRIPE, DATASET_TABLES_SQLITE,
)


if __name__ == '__main__':
    
    # Stripe
    stripe_resource_details = [ {'data_source': data_source} for data_source in STRIPE_DATA_SOURCES_INCREMENTAL_CREATED ]

    create_pipeline(
        name='stripe_to_file',
        destination='filesystem',
        dataset='stripe',
        create_resource_function=create_stripe_resource_incremental_created,
        resources_details=stripe_resource_details,
    )

    # sqlite
    db_path = copy_sqlite_db()
    sqlite_resource_details = [ {'data_source': data_source, 'db_path': db_path} for data_source in SQLITE_DATA_SOURCES_INCREMENTAL_UPDATED_AT ]

    create_pipeline(
        name='sqlite_to_file',
        destination='filesystem',
        dataset='sqlite',
        create_resource_function=create_sqliteDB_resource_incremental_updated_at,
        resources_details=sqlite_resource_details,
    )

    # File to BQ
    datasets_tables_files_to_bq = [DATASET_TABLES_STRIPE,DATASET_TABLES_SQLITE]

    for dataset_tables in datasets_tables_files_to_bq:
        file_to_bq_pipeline(dataset_tables=dataset_tables)

    pretty_all_jsons()