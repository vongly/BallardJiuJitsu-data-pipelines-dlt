import dlt

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from scriptsLoad.from_file import (
    make_file_resource,
    move_processed_file,
    find_files_for_dataset_tables,
    DATASET_TABLES_STRIPE,
    DATASET_TABLES_SQLITE,
)
from env import (
    BQ_SERVICE_ACCOUNT_JSON_PATH,
    PIPELINES_DIR,
    EXTRACT_FILEPATH,
)
from utils import print_pipeline_details

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_SERVICE_ACCOUNT_JSON_PATH

def pipeline(dataset_tables):
    dataset = dataset_tables['dataset']

    pipeline_object = dlt.pipeline(
        pipeline_name=f'{ dataset }_file_to_bq',
        destination='bigquery',
        dataset_name=dataset,
        pipelines_dir=PIPELINES_DIR,
    )

    dataset_tables_w_file_details = find_files_for_dataset_tables(dataset_tables=dataset_tables, extract_filepath=EXTRACT_FILEPATH)

    print_pipeline_details(pipeline_object)
    if dataset_tables_w_file_details['tables'] == []:
        print('\n', f' No new files to process - { dataset }', '\n')
    else:
        for table_w_file_details in dataset_tables_w_file_details['tables']:
            filepath = table_w_file_details['filepath']
            file_directory = table_w_file_details['file_directory']
            processed_directory = f'{ file_directory }/processed'
            print('\n',' Processing:', filepath)

            new_resource = make_file_resource(table_w_file_details=table_w_file_details)
            load_info = pipeline_object.run(new_resource)

            if load_info.load_packages and all(pkg.state == "loaded" for pkg in load_info.load_packages):
                move_processed_file(filepath, processed_directory)
                print('  Finished loading - file moved to processed: ', processed_directory, '\n')
            else:
                print(f' Failed to load: { filepath } — skipping move.', '\n')


if __name__ == '__main__':
    datasets_tables = [DATASET_TABLES_STRIPE,DATASET_TABLES_SQLITE]

    for dataset_tables in datasets_tables:
        pipeline(dataset_tables=dataset_tables)
