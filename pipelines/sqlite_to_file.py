import dlt

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from scriptsExtract.sqliteDB import (
    copy_sqlite_db,
    create_sqliteDB_resource_incremental_updated,
    SQLITE_TABLES_INCREMENTAL_UPDATED_AT,
)
from env import PIPELINES_DIR
from utils import print_pipeline_details

os.environ['DLT_SECRETS_FILE'] = os.path.join(os.path.dirname(__file__), 'secrets.toml')
os.environ['DLT_CONFIG_FILE'] = os.path.join(os.path.dirname(__file__), 'config.toml')


def pipeline(sqlite_tables_incremental_updated_at):
    pipeline_object = dlt.pipeline(
        pipeline_name='sqlite_to_file',
        destination='filesystem',
        dataset_name='sqlite',
        pipelines_dir=PIPELINES_DIR,
    )

    print_pipeline_details(pipeline_object)
    db_path = copy_sqlite_db()

    @dlt.source
    def source_object(sqlite_tables_incremental_updated_at=None):
        if sqlite_tables_incremental_updated_at == None:
            sqlite_tables_incremental_updated_at = pipeline_object.sqlite_tables_incremental_updated_at
        for table in sqlite_tables_incremental_updated_at:
            yield create_sqliteDB_resource_incremental_updated(db_path=db_path, table=table)
        print('\n')

    pipeline_object.sqlite_tables_incremental_updated_at = sqlite_tables_incremental_updated_at
    
    load_info = pipeline_object.run(source_object())
    print('\n', load_info)

    return load_info


if __name__ == '__main__':
    pipeline(sqlite_tables_incremental_updated_at=SQLITE_TABLES_INCREMENTAL_UPDATED_AT)