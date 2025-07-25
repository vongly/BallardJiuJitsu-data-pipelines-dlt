import dlt

import sys
from pathlib import Path
import gzip
import json

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from utils.scriptsLoad.from_file import find_pipeline_data_source_file_details
from utils.helpers import move_file, get_file_type_from_dir

from env import (
    PIPELINES_DIR,
    EXTRACT_DIR,
)
from utils.helpers import (
    print_pipeline_details,
    create_resource_details_w_kwargs,
    make_list_if_not,
    make_dictionary,
)


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

        self.kwargs = kwargs
        data_sources = make_list_if_not(kwargs.get('data_sources', []))
        data_sources = [{'data_source': data_source} for data_source in data_sources ]
 
        self.pipeline_details = make_dictionary(
            pipeline_name=pipeline_name,
            extract_pipeline_name=extract_pipeline_name,
            dataset=dataset,
            data_sources=data_sources,
        )

        self.pipeline_details_w_file_details = find_pipeline_data_source_file_details(pipeline_details=self.pipeline_details, extract_dir=EXTRACT_DIR)
        '''
           find_pipeline_data_source_file_details() -> determines location of data files
            - uses env.EXTRACT_DIR and destination variable
        '''

    def run_pipelines(self):
        print_pipeline_details(self.pipeline_object)
        jobs = []
        if self.pipeline_details_w_file_details['data_sources'] != []:
            for data_source_w_file_details in self.pipeline_details_w_file_details['data_sources']:

                data_source = data_source_w_file_details['data_source']
                data_source_dir = data_source_w_file_details['data_source_dir']

                resource_details = create_resource_details_w_kwargs(
                    kwargs_input=self.kwargs,
                    pipeline_name=self.pipeline_name,
                    data_source=data_source,
                    data_source_dir=data_source_dir
                )

                new_resource = self.create_resource_function(resource_details)
                load_info = self.pipeline_object.run(new_resource)
                load_packages = load_info.load_packages

                for package in load_packages:
                    completed_jobs = package.jobs.get('completed_jobs', [])
                    failed_jobs = package.jobs.get('failed_jobs', [])

                    for job in completed_jobs + failed_jobs:
                        job_details = {}
                        job_details['table'] = getattr(job.job_file_info, 'table_name', None)
                        job_details['state'] = getattr(job, 'state', None)
                        job_details['file_path'] = getattr(job, 'file_path', None)
                        job_details['MB'] = round(getattr(job, 'file_size', None)/1000000, 2) if getattr(job, 'file_size', None) else None
                        job_details['seconds_elapsed'] = round(getattr(job, 'elapsed', None), 2) if getattr(job, 'elapsed', None) else None

                        with gzip.open(job.file_path, 'rt', encoding='utf-8') as f:
                            job_details['records'] = sum(1 for _ in f)
                        jobs.append(job_details)

                '''
                    Moves files to a processed folder after they've been loaded
                '''
                if load_packages and all(pkg.state == 'loaded' for pkg in load_info.load_packages):
                    '''
                        success - move file to processed folder
                    '''
                    processed_directory = data_source_dir / 'processed'
                    files = get_file_type_from_dir(data_source_dir,'jsonl')
                    for file in files:
                        move_file(file, processed_directory)
                else:
                    '''
                        no load package - nothing was loaded
                    '''
                    pass
        else:
            '''
                no files to process
            '''
            pass

        self.jobs = sorted(jobs, key=lambda x: x['table'])
        self.jobs_json = json.dumps(self.jobs, indent=2)

    def run_all(self):
        self.run_pipelines()