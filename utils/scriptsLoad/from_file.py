import dlt

import sys
from pathlib import Path
import gzip, json
import time

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from helpers import get_file_type_from_dir


# Identifies the filepaths of extracted data files
    ### need to sync/generalize file structure
def find_pipeline_data_source_file_details(pipeline_details, extract_dir):
    extract_pipeline = pipeline_details['extract_pipeline_name']
    dataset = pipeline_details['dataset']
    extract_dir = Path(extract_dir)

    for data_source in pipeline_details['data_sources'][:]:
        data_source_name = data_source['data_source']

        DATA_SOURCE_DIR = extract_dir / extract_pipeline / dataset / data_source_name
        print(str(DATA_SOURCE_DIR))

        if not DATA_SOURCE_DIR.exists():
            pipeline_details['data_sources'].remove(data_source)
            continue

        has_jsonl_files = list(DATA_SOURCE_DIR.glob("*.jsonl"))

        if not has_jsonl_files:
            pipeline_details['data_sources'].remove(data_source)
            continue

        data_source['data_source_dir'] = DATA_SOURCE_DIR

    return pipeline_details


# Loads all gzip jsonl file for one dataset/table
def load_jsonl_gzip(directory):
    start = time.time()

    files = get_file_type_from_dir(directory,'jsonl')
    count = 0
    for file in files:
        with gzip.open(str(file), 'rt', encoding='utf-8') as f:
            for line in f:
                count += 1
                record = json.loads(line)
                record["_source_file"] = file.name
                yield record

    end = time.time()
    print(f'  Record Count -', count, f'({end - start:.1f}s)')

# Generates a resource from file
# creates one resource for one source
def create_file_resource(resource_details):
    pipeline_name = resource_details['pipeline_name']
    data_source = resource_details['data_source']
    data_source_dir = resource_details['data_source_dir']

    table_name = f'sqlite_{ data_source }_incremental_updated_at'
    resource_name = f'{ pipeline_name }__{ table_name }'
    @dlt.resource(name=resource_name, table_name=table_name, write_disposition='append')
    def created_resource():
        yield from load_jsonl_gzip(directory=data_source_dir)

    return created_resource
