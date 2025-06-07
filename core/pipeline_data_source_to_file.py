import dlt

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import PIPELINES_DIR, EXTRACT_DIR
from utils.helpers import (
    print_pipeline_details,
    create_resource_details_w_kwargs,
    make_list_if_not,
)


class CreateDataSourceToFilePipeline:
    def __init__(
            self,
            pipeline_name: str,
            destination: object,
            dataset: str,
            create_resource_function: callable,
            pipelines_dir=PIPELINES_DIR,
            **kwargs,
        ):
        self.pipeline_name = pipeline_name
        self.destination = dlt.destinations.filesystem(bucket_url=destination)
        self.dataset = dataset
        self.create_resource_function = create_resource_function
        self.pipelines_dir = pipelines_dir

        self.pipeline_object = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            destination=self.destination,
            dataset_name=self.dataset,
            pipelines_dir=self.pipelines_dir,
        )

        data_sources = make_list_if_not(kwargs.get('data_sources', []))
 
        self.resources_details = []
        for data_source in data_sources:
            resource_details = create_resource_details_w_kwargs(
                kwargs_input=self.kwargs,
                pipeline_name=self.pipeline_name,
                data_source=data_source,
            )
            self.resources_details.append(resource_details)

    def run_pipeline(self):
        print_pipeline_details(self.pipeline_object)

        resources = []

        for details in self.resources_details:
            resource = self.create_resource_function(details)
            resource.buffer_max_items = 1000
            resources.append(resource)

        load_info = self.pipeline_object.run(resources)
        print('\n', load_info)

        return load_info

    def run_all(self):
        self.run_pipeline()
