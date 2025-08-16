import dlt
from dlt.sources import incremental

import paramiko
from scp import SCPClient
import sqlite3

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))


class AccessSqliteDB:
    def __init__(
        self,
        private_key_path,
        sqlite_loc_ip,
        sqlite_loc_user,
        sqlite_loc_file_path,
        sqlite_destination,
    ):

        self.private_key = paramiko.Ed25519Key.from_private_key_file(private_key_path)
        self.sqlite_loc_ip = sqlite_loc_ip
        self.sqlite_loc_user = sqlite_loc_user

        self.sqlite_loc_file_path = sqlite_loc_file_path
        self.sqlite_destination = sqlite_destination

    def copy_sqlite_db(self):

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=self.sqlite_loc_ip,
            username=self.sqlite_loc_user,
            pkey=self.private_key,
            port=22
        )

        if not os.path.exists(self.sqlite_destination):
            os.makedirs(self.sqlite_destination)

        with SCPClient(ssh.get_transport()) as scp:
            db_path = os.path.join(self.sqlite_destination, 'bjj_copy.sqlite')
            print(f'  Copying sqlite db to: { db_path }', '\n')
            scp.get(self.sqlite_loc_file_path, db_path)
        
        ssh.close()

        return db_path

class SqliteResource:
    def __init__(
            self,
            data_source,
            db_path,
            incremental_attribute=None,
            where_clause=None,
            **kwargs    
    ):

        self.data_source = data_source
        self.incremental_obj = incremental(incremental_attribute, None)
        self.db_path = db_path
        self.where_clause = where_clause

        # **kwargs
        self.table_name_suffix = kwargs.get('table_name_suffix', '')


    def yield_query_results(self, incremental_obj=None, where_clause=None):

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        where_clause = '' if self.where_clause == None else self.where_clause

        if incremental_obj:
            if incremental_obj.last_value:
                if where_clause:
                    where_clause = where_clause + f''' AND updated_at > '{ incremental_obj.last_value }' '''
                else:
                    where_clause = f''' WHERE updated_at > '{ incremental_obj.last_value }' '''

        cursor.execute(f'SELECT *, CURRENT_TIMESTAMP as _dlt_processed FROM { self.data_source } { where_clause }')
        columns = [ col[0] for col in cursor.description ]

        for row in cursor.fetchall():
            yield dict(zip(columns, row))

        conn.close()


    def create_resource(self):
        table_name = self.data_source + self.table_name_suffix
        @dlt.resource(name=self.data_source, table_name=table_name, write_disposition='append', primary_key=None)
        def my_resource(incremental_obj=self.incremental_obj):
            '''
                primary_key=None -> to record history of slow changing fields
            '''
            yield self.yield_query_results(incremental_obj)
        return my_resource()
