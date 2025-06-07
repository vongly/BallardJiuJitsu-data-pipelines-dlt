import os, sys
from pathlib import Path
import json
import shutil

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    EXTRACT_DIR,
    PROJECT_DIRECTORY,
)

def move_file(source, destination):
    source = Path(source)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    shutil.move(source, destination)

    return {'source': str(source), 'destination': str(destination)}

def get_file_type_from_dir(directory, filetype):
    files = directory.glob(f'*.{ filetype }')

    return files

def print_pipeline_details(pipeline):
    pipeline_name = pipeline.pipeline_name
    destination = pipeline.destination.__class__.__name__.lower()
    dataset = pipeline.dataset_name
    working_dir = pipeline.pipelines_dir

    print('\n', ' RUNNING NEW PIPELINE -')
    print('  Name:', pipeline_name)
    if destination == 'filesystem':
        print('  Destination:', EXTRACT_DIR)
    else:
        print('  Destination:', destination)
    print('  Dataset:', dataset)
    print('  Working dir:', working_dir, '\n')

def pretty_print_json_file(input_path, output_path):
    try:
        with open(input_path, 'r') as infile:
            data = json.load(infile)

        with open(output_path, 'w') as outfile:
            json.dump(data, outfile, indent=2)

    except Exception as e:
        pass

def pretty_all_jsons(base_dir=PROJECT_DIRECTORY):
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.json'):
                input_path = os.path.join(root, file)
                output_path = os.path.join(root, file)
                pretty_print_json_file(input_path, output_path)

def make_list_if_not(my_list):
    if not isinstance(my_list, list):
        my_list = [my_list]
    return my_list

def create_resource_details_w_kwargs(kwargs_input,excluded_args=['data_sources'],**kwargs):

    excluded_args = make_list_if_not(excluded_args)

    resource_details = {}

    for key, value in kwargs.items():
        resource_details[key] = value    
    for key, value in kwargs_input.items():
        if key not in excluded_args:
            resource_details[key] = value    

    return resource_details

def make_dictionary(**kwargs):
    return kwargs

if __name__ == "__main__":
    pretty_all_jsons()
