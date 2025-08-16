import dlt

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core import create_pipeline
from utils.resources.sqliteDB import (
    SqliteResource,
    AccessSqliteDB,
)
from env import (
    EXTRACT_DIR,
    SSH_KEY_PATH_FOR_SQLITE,
    SQLITE_LOCATION_IP,
    SQLITE_LOCATION_USER,
    SQLITE_LOCATION_FILEPATH,
    PROJECT_DIRECTORY,
)


def run_sqlite_to_file_pipeline():

    data_sources_sqlite = [
        'users',
        'kids',
        'class_times',
        'class_time_checkins',
        'kids_class_time_checkins',
    ]

    db_path = AccessSqliteDB(
        private_key_path=SSH_KEY_PATH_FOR_SQLITE,
        sqlite_loc_ip=SQLITE_LOCATION_IP,
        sqlite_loc_user=SQLITE_LOCATION_USER,
        sqlite_loc_file_path=SQLITE_LOCATION_FILEPATH,
        sqlite_destination=PROJECT_DIRECTORY,
    ).copy_sqlite_db()

    resources = [
        SqliteResource(
            data_source=data_source,
            db_path=db_path,
            incremental_attribute='updated_at',
            table_name_suffix='__incr_updated_at',
        ).create_resource()

            for data_source in data_sources_sqlite
    ]


    pipeline_name = 'sqlite_to_file'
    destination = f'{ EXTRACT_DIR }/{ pipeline_name }'
    dataset ='sqlite'

    pipeline = create_pipeline.Pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.filesystem(bucket_url=destination),
        dataset=dataset,
        resources=resources,
    )

    pipeline.run_pipeline()
    return pipeline

if __name__ == '__main__':
    pipeline = run_sqlite_to_file_pipeline()
    print(pipeline.jobs_json)