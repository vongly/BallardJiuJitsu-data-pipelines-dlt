import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline_data_source_to_file import create_data_source_to_file_pipeline
from utils.scriptsExtract.sqliteDB import (
    create_sqliteDB_resource_incremental_updated_at,
    copy_sqlite_db,
)


def run_sqlite_to_file_pipeline():

    SQLITE_DATA_SOURCES_INCREMENTAL_UPDATED_AT = [
        'users',
        'kids',
        'class_times',
        'class_time_checkins',
        'kids_class_time_checkins',
    ]

    db_path = copy_sqlite_db()
    sqlite_resource_details = [ {'data_source': data_source, 'db_path': db_path} for data_source in SQLITE_DATA_SOURCES_INCREMENTAL_UPDATED_AT ]

    create_data_source_to_file_pipeline(
        name='sqlite_to_file',
        destination='filesystem',
        dataset='sqlite',
        create_resource_function=create_sqliteDB_resource_incremental_updated_at,
        resources_details=sqlite_resource_details,
    )

if __name__ == '__main__':
    run_sqlite_to_file_pipeline()