import dlt

import gzip, json
import shutil
import hashlib
from pathlib import Path
import time


# Moves processed file after pipeline is ran
def move_processed_file(source, destination):
    output_dir = Path(destination)
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(source, destination)

# Identifies the filepaths of extracted data files
    ### need to sync/generalize file structure
def find_files_for_dataset_tables(dataset_tables, extract_filepath):
    pipeline = dataset_tables['pipeline']
    dataset = dataset_tables['dataset']
    for table in dataset_tables['tables'][:]:
        table_name = table['table']

        FILE_DIR_PATH_STR = f'{ extract_filepath }/{ pipeline }/{ dataset }/{ table_name }'
        FILE_DIR_PATH = Path(FILE_DIR_PATH_STR)

        if not FILE_DIR_PATH.exists():
            dataset_tables['tables'].remove(table)
            continue

        filenames = [ f.name for f in FILE_DIR_PATH.iterdir() if f.suffix == '.jsonl' ]

        if filenames == []:
            dataset_tables['tables'].remove(table)
            continue

        for filename in filenames:
            ABS_FILEPATH = f'{ FILE_DIR_PATH }/{ filename }'

            table['file_directory'] = FILE_DIR_PATH_STR
            table['filename'] = filename
            table['filepath'] = ABS_FILEPATH
                    
    return dataset_tables

# Loads records gzip jsonl file for each file -> file format from dlt
def load_jsonl_gzip(table_w_file_details):
    start = time.time()
    table = table_w_file_details['table']
    filename = table_w_file_details['filename']
    filepath = table_w_file_details['filepath']

    count = 0
    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
        for line in f:
            count += 1
            record = json.loads(line)
            record["_source_file"] = filename
            yield record
    end = time.time()
    print(f' Record Count: { table } -', count, f'({end - start:.1f}s)')

# Generic resource generator
# creates a different resource for each file regardless of source
def create_file_resource(table_w_file_details):
    filename = table_w_file_details['filename']
    table = table_w_file_details['table']
    suffix = hashlib.md5(str(filename).encode()).hexdigest()[:8] # suffix for resource name
    resource_name = f'{ table }_{ suffix }'

    @dlt.resource(name=resource_name, table_name=table, write_disposition='append')
    def created_resource():
        yield from load_jsonl_gzip(table_w_file_details=table_w_file_details)

    return created_resource

if __name__ == '__main__':
    import sys
    from pathlib import Path

    parent_dir = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(parent_dir))

    from env import EXTRACT_FILEPATH

    dataset_tables_stripe = {
        'dataset': 'stripe',
        'tables': [
            {'table': 'stripe_refunds_incremental_id'},
        ]
    }
    dataset_tables_sqlite = {
        'dataset': 'sqlite',
        'tables': [
            {'table': 'sqlite_users_incremental_updated_at'},
        ]
    }

    datasets_tables = [dataset_tables_stripe, dataset_tables_sqlite]

    for dataset_tables in datasets_tables:
        dataset = dataset_tables['dataset']
        dataset_tables_w_file_details = find_files_for_dataset_tables(dataset_tables=dataset_tables, extract_filepath=EXTRACT_FILEPATH)

        if dataset_tables_w_file_details['tables'] == []:
            print('\n', f' No new files to process - { dataset }', '\n')
        else:
            for table_w_file_details in dataset_tables_w_file_details['tables']:
                filepath = table_w_file_details['filepath']
                file_directory = table_w_file_details['file_directory']
                processed_directory = f'{ file_directory }/processed'
                print('\n',' Processing:', filepath)

        
                records = load_jsonl_gzip(table_w_file_details=table_w_file_details)
                for r in records:
                    print(r)