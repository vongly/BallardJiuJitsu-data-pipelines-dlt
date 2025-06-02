import os
import json

from env import (
    EXTRACT_FILEPATH,
    PROJECT_DIRECTORY,
)

def print_pipeline_details(pipeline):
    pipeline_name = pipeline.pipeline_name
    destination = pipeline.destination.__class__.__name__.lower()
    dataset = pipeline.dataset_name
    working_dir = pipeline.pipelines_dir

    print('\n')
    print('  Pipeline -')
    print('  Name:', pipeline_name)
    if destination == 'filesystem':
        print('  Destination:', EXTRACT_FILEPATH)
    else:
        print('  Destination:', destination)
    print('  Dataset:', dataset)
    print('  Working dir:', working_dir)


def pretty_print_json_file(input_path, output_path):
    try:
        with open(input_path, 'r') as infile:
            lines = infile.readlines()
            if all(line.strip().startswith('{') for line in lines):
                data = [json.loads(line) for line in lines if line.strip()]
            else:
                infile.seek(0)
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

if __name__ == "__main__":
    pretty_all_jsons()