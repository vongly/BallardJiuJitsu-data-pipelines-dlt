from utils import pretty_all_jsons

from pipelines.stripe_to_file import (
    pipeline as stripe_to_file_pipeline, STRIPE_ENDPOINTS_INCREMENTAL_ID,
)
from pipelines.sqlite_to_file import (
    pipeline as sqlite_to_file_pipeline, SQLITE_TABLES_INCREMENTAL_UPDATED_AT
)
from pipelines.file_to_bq import (
    pipeline as file_to_bq_pipeline, DATASET_TABLES_STRIPE, DATASET_TABLES_SQLITE,
)


if __name__ == '__main__':
    # Sources to file
    stripe_to_file_pipeline(STRIPE_ENDPOINTS_INCREMENTAL_ID)
    sqlite_to_file_pipeline(SQLITE_TABLES_INCREMENTAL_UPDATED_AT)

    # File to BQ
    datasets_tables_files_to_bq = [DATASET_TABLES_STRIPE,DATASET_TABLES_SQLITE]

    for dataset_tables in datasets_tables_files_to_bq:
        file_to_bq_pipeline(dataset_tables=dataset_tables)

    pretty_all_jsons()