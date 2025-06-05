import dlt
from dlt.sources import incremental

import paramiko
from scp import SCPClient
import sqlite3

import os
import sys
from pathlib import Path
import time

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))

from env import (
    SSH_KEY_PATH_FOR_SQLITE,
    SQLITE_LOCATION_IP,
    SQLITE_LOCATION_USER,
    SQLITE_LOCATION_FILEPATH,
    PROJECT_DIRECTORY,
)


def create_ssh_client():
    PRIVATE_KEY = paramiko.Ed25519Key.from_private_key_file(SSH_KEY_PATH_FOR_SQLITE)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=SQLITE_LOCATION_IP,
        username=SQLITE_LOCATION_USER,
        pkey=PRIVATE_KEY,
        port=22
    )

    return ssh

def copy_sqlite_db():
    if not os.path.exists(PROJECT_DIRECTORY):
        os.makedirs(PROJECT_DIRECTORY)

    ssh = create_ssh_client()

    with SCPClient(ssh.get_transport()) as scp:
        db_path = os.path.join(PROJECT_DIRECTORY, 'bjj_copy.sqlite')
        print(f'  Copying sqlite db to: { db_path }', '\n')
        scp.get(SQLITE_LOCATION_FILEPATH, db_path)
    
    ssh.close()

    return db_path


def query_sqlite_incremental_updated_at(resource_name, data_source, db_path, incremental_value=None, where_clause = None):
    start = time.time()
    print(f'  Processing - { resource_name }')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    where_clause = '' if where_clause == None else where_clause

    if incremental_value:
        if incremental_value.last_value:
            where_clause = f''' WHERE updated_at > '{ incremental_value.last_value }' '''

    cursor.execute(f'SELECT * FROM { data_source } { where_clause }')
    columns = [ col[0] for col in cursor.description ]

    count = 0
    for row in cursor.fetchall():
        count += 1
        yield dict(zip(columns, row))

    conn.close()
    end = time.time()
    print(f'  Record Count - { resource_name }:', count, f'({end - start:.1f}s)')

def create_sqliteDB_resource_incremental_updated_at(resource_details):
    pipeline_name = resource_details['pipeline_name']
    data_source = resource_details['data_source']
    db_path = resource_details['db_path']

    table_name = f'sqlite_{ data_source }_incremental_updated_at'
    resource_name = f'{ pipeline_name }__{ table_name }'

    @dlt.resource(name=resource_name, table_name=table_name, write_disposition='append', primary_key=None)
    def create_resource(incremental_value=incremental('updated_at', initial_value=None)):
        '''
            primary_key=None -> to record history of slow changing fields
        '''
        yield query_sqlite_incremental_updated_at(resource_name, data_source, db_path, incremental_value)
    return create_resource()
