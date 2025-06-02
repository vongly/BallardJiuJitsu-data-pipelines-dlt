import dlt
from dlt.sources import incremental

import paramiko
from scp import SCPClient
import sqlite3

import os
import sys
from pathlib import Path
import time

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    SSH_KEY_PATH_FOR_SQLITE,
    SQLITE_LOCATION_IP,
    SQLITE_LOCATION_USER,
    SQLITE_LOCATION_FILEPATH,
    PROJECT_DIRECTORY,
)

SQLITE_TABLES_INCREMENTAL_UPDATED_AT = [
    'users',
    'kids',
    'class_times',
    'class_time_checkins',
    'kids_class_time_checkins',
]

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


def query_sqlite_table(db_path, table, incremental_updated_at=None, where_clause = None):
    start = time.time()
    print(f'  Processing - { table }')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    where_clause = '' if where_clause == None else where_clause

    if incremental_updated_at:
        if incremental_updated_at.last_value:
            where_clause = f''' WHERE updated_at > '{ incremental_updated_at.last_value }' '''

    cursor.execute(f'SELECT * FROM { table } { where_clause }')
    columns = [ col[0] for col in cursor.description ]

    count = 0
    for row in cursor.fetchall():
        count += 1
        yield dict(zip(columns, row))

    conn.close()
    end = time.time()
    print(f'  Record Count: { table } -', count, f'({end - start:.1f}s)')

def create_sqliteDB_resource_incremental_updated(db_path, table):
    resource_name = f'sqlite_{ table }_incremental_updated_at'
    @dlt.resource(name=resource_name, write_disposition='append')
    def create_resource(incremental_updated_at=incremental('updated_at', initial_value=None)):
        yield query_sqlite_table(db_path, table, incremental_updated_at)
    return create_resource

if __name__ == '__main__':
    db_path = copy_sqlite_db

    records = query_sqlite_table(
        db_path = db_path,
        table='users',
        incremental_updated_at=None,
    )

    for r in records:
        print(r)