import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline_data_source_to_file import CreateDataSourceToFilePipeline
from utils.scriptsExtract.sqliteDB import (
    create_sqliteDB_resource_incremental_updated_at,
    copy_sqlite_db,
)
from env import EXTRACT_DIR


def run_sqlite_to_file_pipeline():

    SQLITE_DATA_SOURCES_INCREMENTAL_UPDATED_AT = [
        'users',
        'kids',
        'class_times',
        'class_time_checkins',
        'kids_class_time_checkins',
    ]

    db_path = copy_sqlite_db()
    pipeline_name = 'sqlite_to_file'
    destination = f'{ EXTRACT_DIR }/{ pipeline_name }'
    dataset ='sqlite'

    pipeline = CreateDataSourceToFilePipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset=dataset,
        create_resource_function=create_sqliteDB_resource_incremental_updated_at,
        data_sources=SQLITE_DATA_SOURCES_INCREMENTAL_UPDATED_AT,
        db_path=db_path,
    )

    pipeline.run_all()

    return pipeline

if __name__ == '__main__':
    run_sqlite_to_file_pipeline()