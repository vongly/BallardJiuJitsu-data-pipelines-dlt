import dlt

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from utils.scriptsLoad.from_file import (
    move_processed_file,
    find_files_for_dataset_tables,
)
from env import (
    PIPELINES_DIR,
    EXTRACT_FILEPATH,
)
from utils.helpers import print_pipeline_details

'''
    dataset_tables_example = {
        'pipeline': 'pipeline_name',
        'dataset': 'dataset_name',
        'tables': [
            {'table': 'table_name_must_match_resource_name_1'},
            {'table': 'table_name_must_match_resource_name_2'},
            {'table': 'table_name_must_match_resource_name_3'},
            ...
            ...
            ...
            {'table': 'table_name_must_match_resource_name_n'},
        ]
    }
'''

def create_file_to_destination_pipeline(
        name: str,
        destination: str,
        create_resource_function: callable,
        dataset_tables: dict,
        pipelines_dir=PIPELINES_DIR,
    ):
    dataset = dataset_tables['dataset']

    pipeline_object = dlt.pipeline(
        pipeline_name=name,
        destination=destination,
        dataset_name=dataset,
        pipelines_dir=pipelines_dir,
    )

    dataset_tables_w_file_details = find_files_for_dataset_tables(dataset_tables=dataset_tables, extract_filepath=EXTRACT_FILEPATH)

    print_pipeline_details(pipeline_object)

    if dataset_tables_w_file_details['tables'] == []:
        print(f'\n  No new files to process - { dataset }', '\n')
    else:
        for table_w_file_details in dataset_tables_w_file_details['tables']:
            filepath = table_w_file_details['filepath']
            file_directory = table_w_file_details['file_directory']
            processed_directory = f'{ file_directory }/processed'
            print('\n  Processing:', filepath)

            new_resource = create_resource_function(table_w_file_details=table_w_file_details)
            load_info = pipeline_object.run(new_resource)

            if load_info.load_packages and all(pkg.state == 'loaded' for pkg in load_info.load_packages):
                move_processed_file(filepath, processed_directory)
                print('  Finished loading - file moved to processed: ', processed_directory, '\n')
            else:
                print(f' Failed to load: { filepath } — skipping move.', '\n')

