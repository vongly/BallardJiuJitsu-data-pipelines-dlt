import dlt

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import PIPELINES_DIR, EXTRACT_DIR
from utils.helpers import print_pipeline_details


class CreateDataSourceToFilePipeline:
    def __init__(
            self,
            pipeline_name: str,
            destination: str,
            dataset: str,
            create_resource_function: callable,
            pipelines_dir=PIPELINES_DIR,
            **kwargs,
        ):
        self.pipeline_name = pipeline_name
        self.destination = destination
        self.dataset = dataset
        self.create_resource_function = create_resource_function
        self.pipelines_dir = pipelines_dir


        self.pipeline_object = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            dataset_name=self.dataset,
            pipelines_dir=self.pipelines_dir,
        )

        data_sources = kwargs.get('data_sources', [])
        if not isinstance(data_sources, list):
            data_sources = [data_sources]
 
        self.resources_details = []
        for data_source in data_sources:
            resource_details_item = {}
            resource_details_item['data_source'] = data_source
            resource_details_item['pipeline_name'] = pipeline_name
            for key, value in kwargs.items():
                if key not in ['data_sources']:
                    resource_details_item[key] = value
            self.resources_details.append(resource_details_item)

    def run_pipeline(self):
        print_pipeline_details(self.pipeline_object)
        resources = [
            self.create_resource_function(details) for details in self.resources_details
        ]
        load_info = self.pipeline_object.run(resources)
        print('\n', load_info)

        return load_info

    def run_all(self):
        self.run_pipeline()
