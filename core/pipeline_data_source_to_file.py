import dlt

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import PIPELINES_DIR
from utils.helpers import print_pipeline_details

# Required in all pipelines with destination to filesystem
os.environ['DLT_SECRETS_FILE'] = os.path.join(os.path.dirname(__file__), 'secrets.toml')
os.environ['DLT_CONFIG_FILE'] = os.path.join(os.path.dirname(__file__), 'config.toml')

def create_data_source_to_file_pipeline(
        name: str,
        dataset: str,
        create_resource_function: callable,
        resources_details: list[dict],
        destination='filesystem',
        pipelines_dir=PIPELINES_DIR,
    ):

    pipeline_object = dlt.pipeline(
        pipeline_name=name,
        destination=destination,
        dataset_name=dataset,
        pipelines_dir=pipelines_dir,
    )

    print_pipeline_details(pipeline_object)
    resources = [ create_resource_function(**kwargs) for kwargs in resources_details ]
    load_info = pipeline_object.run(resources)
        
    print('\n', load_info)
