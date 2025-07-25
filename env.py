from google.oauth2 import service_account
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[0] / '.env'
load_dotenv(dotenv_path=env_path)

PROJECT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

BQ_SERVICE_ACCOUNT_JSON_FILENAME = os.environ['BQ_SERVICE_ACCOUNT_JSON_FILENAME']
BQ_SERVICE_ACCOUNT_JSON_PATH = os.path.join(PROJECT_DIRECTORY, BQ_SERVICE_ACCOUNT_JSON_FILENAME)
CREDENTIALS_BQ = service_account.Credentials.from_service_account_file(BQ_SERVICE_ACCOUNT_JSON_PATH)

POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_CERTIFICATION = os.environ['POSTGRES_CERTIFICATION']
POSTGRES_CERTIFICATION_PATH = os.path.join(PROJECT_DIRECTORY, POSTGRES_CERTIFICATION)

STRIPE_API_SECRET = os.environ['STRIPE_API_SECRET']

PIPELINES_DIR_RELATIVE = os.environ['PIPELINES_DIR_RELATIVE']
PIPELINES_DIR = Path(__file__).parent / PIPELINES_DIR_RELATIVE
EXTRACT_DIR = os.environ['EXTRACT_DIR']

SSH_KEY_PATH_FOR_SQLITE = os.environ['SSH_KEY_PATH_FOR_SQLITE']
SQLITE_LOCATION_IP = os.environ['SQLITE_LOCATION_IP']
SQLITE_LOCATION_USER = os.environ['SQLITE_LOCATION_USER']
SQLITE_LOCATION_USER = os.environ['SQLITE_LOCATION_USER']
SQLITE_LOCATION_FILEPATH = os.environ['SQLITE_LOCATION_FILEPATH']


if __name__ == '__main__':
    for var_name, value in list(locals().items()):
        print(f"{var_name} = {value}")
    print('\n')
