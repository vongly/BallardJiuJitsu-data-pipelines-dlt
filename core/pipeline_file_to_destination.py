import dlt

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from utils.scriptsLoad.from_file import find_pipeline_data_source_file_details
from utils.helpers import move_file, get_file_type_from_dir

from env import (
    PIPELINES_DIR,
    EXTRACT_DIR,
)
from utils.helpers import print_pipeline_details


class CreateFileToDistinationPipeline:
    def __init__(
        self,
        pipeline_name: str,
        destination: str,
        dataset: str,
        create_resource_function: callable,
        extract_pipeline_name: str,
        pipelines_dir=PIPELINES_DIR,
        **kwargs,
        ):

        self.pipeline_name = pipeline_name
        self.destination = destination
        self.dataset = dataset
        self.create_resource_function = create_resource_function
        self.extract_pipeline_name = extract_pipeline_name
        self.pipelines_dir=pipelines_dir

        self.pipeline_object = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            dataset_name=self.dataset,
            pipelines_dir=self.pipelines_dir,
        )


        data_sources = kwargs.get('data_sources', [])
        if not isinstance(data_sources, list):
            data_sources = [data_sources]
        data_sources = [{'data_source': data_source} for data_source in data_sources ]
 
        self.pipeline_details = {}
        self.pipeline_details['pipeline_name'] = pipeline_name
        self.pipeline_details['extract_pipeline_name'] = extract_pipeline_name
        self.pipeline_details['dataset'] = dataset
        self.pipeline_details['data_sources'] = data_sources

        self.pipeline_details_w_file_details = find_pipeline_data_source_file_details(pipeline_details=self.pipeline_details, extract_dir=EXTRACT_DIR)
        '''
           find_pipeline_data_source_file_details() -> determines location of data files
           - based off of
                - config.toml
                - files must have the same base directory
                - env.EXTRACT_DIR must be pointed to base directory of config.toml
        '''

    def run_pipelines(self):
        print_pipeline_details(self.pipeline_object)
        if self.pipeline_details_w_file_details['data_sources'] != []:
            for data_source_w_file_details in self.pipeline_details_w_file_details['data_sources']:
                data_source = data_source_w_file_details['data_source']
                data_source_dir = data_source_w_file_details['data_source_dir']

                resource_details = {}
                resource_details['pipeline_name'] = self.pipeline_name
                resource_details['extact_pipeline_name'] = self.pipeline_name
                resource_details['data_source'] = data_source
                resource_details['data_source_dir'] = data_source_dir

                print('\n  Processing:', data_source_dir)

                new_resource = self.create_resource_function(resource_details)
                load_info = self.pipeline_object.run(new_resource)

                if load_info.load_packages and all(pkg.state == 'loaded' for pkg in load_info.load_packages):
                    processed_directory = data_source_dir / 'processed'
                    files = get_file_type_from_dir(data_source_dir,'jsonl')
                    for file in files:
                        move_file(file, processed_directory)
                    print('  Finished loading - file moved to processed: ', processed_directory, '\n')
                else:
                    filepath = str(file)
                    print(f' Failed to load: { filepath } — skipping move.', '\n')
        else:
            print(f'\n  No new files to process - { self.dataset }', '\n')


    def run_all(self):
        self.run_pipelines()