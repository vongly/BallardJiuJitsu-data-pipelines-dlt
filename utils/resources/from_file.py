import dlt
import duckdb

import sys
from pathlib import Path
import gzip, json
from glob import glob
import pyarrow as pa

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from helpers import get_file_type_from_dir


# Identifies the filepaths of extracted data files
    ### need to sync/generalize file structure

class FileResource:
    def __init__(
            self,
            extract_dir,
            file_pipeline_name,
            file_dataset,
            file_data_source,
            file_type='parquet',
    ):

        self.extract_dir = Path(extract_dir)
        self.file_pipeline_name = file_pipeline_name
        self.file_dataset = file_dataset
        self.file_data_source = file_data_source
        
        self.data_source_dir = self.extract_dir / file_pipeline_name / file_dataset / file_data_source

        if self.data_source_dir.exists():
            # Check if directory exists for data sources
            self.files = glob(f'{self.data_source_dir}/*.{file_type}')
        else:
            self.files = []

    def return_file_results(self):
        if self.files != []:
#            files_string = '[' + ', '.join(f"'{file}'" for file in self.files) + ']'
            files_string = [ str(file) for file in self.files ]
            con = duckdb.connect()
            query = "SELECT * FROM read_parquet(?, union_by_name=true)"
            cur = con.execute(query, [files_string])

            try:
                for rb in cur.fetch_record_batch(rows_per_batch=200_000):
                    yield pa.Table.from_batches([rb])
            except AttributeError:
                tbl = cur.arrow()
                for b in tbl.to_batches(max_chunksize=200_000):
                    yield b.to_table()

    # Generates a resource from file
    # creates one resource for one source
    def create_resource(self):
        @dlt.resource(name=self.file_data_source, table_name=self.file_data_source, write_disposition='append')
        def my_resource():
            yield from self.return_file_results()
        return my_resource
