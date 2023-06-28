import os

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from utils import *


class ExtractAndLoad:
    """
        This class downloads data from a public s3 bucket and loads to a postgres DB
    """

    def __init__(self,
                 db_connection: connection,
                 s3_config: S3Config,
                 db_configs: DBConfig
                 ):
        pass


def download_from_s3(bucket_name: str,
                     directory_prefix: str,
                     s3_keys: list[str],
                     download_path: str):
    s3 = boto3.client('s3',
                      region_name=S3_REGION,
                      config=Config(signature_version=UNSIGNED))
    try:
        response = s3.list_objects(Bucket=bucket_name, Prefix=directory_prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                object_key = obj['Key']
                key = object_key.split("/")[-1]
                if key in s3_keys:
                    download_file_location = f"{download_path}/{object_key}"
                    print(f"downloading {key} in {download_file_location}.")
                    os.makedirs(os.path.dirname(download_file_location), exist_ok=True)
                    s3.download_file(bucket_name, object_key, download_file_location)
    except Exception as e:
        print(f"Error while exporting data from s3: {e}")


def create_schemas(schema_names: list[str],
                   db_connection: connection):
    cur = db_connection.cursor()
    for schema_name in schema_names:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    db_connection.close()


def load_csv_files_to_postgres(file_names: list[str],
                               parent_path: str,
                               db_connection: connection):
    try:
        cur = db_connection.cursor()
        for file_name in file_names:
            table_name = file_name.split('.')[0]
            fully_qualified_file_name = f"{parent_path}/{file_name}"
            print(f"loading data from {fully_qualified_file_name} file  to table {table_name}")
            with open(fully_qualified_file_name, 'r') as f:
                next(f)
                cur.copy_from(f, table_name, sep=',')
        db_connection.commit()
        cur.close()
        db_connection.close()

    except Exception as e:
        print(f"Error while loading data to postgres: {e}")


if __name__ == '__main__':
    download_from_s3(BUCKET_NAME, PREFIX, FILE_LIST, DOWNLOAD_PATH)
    load_csv_files_to_postgres(FILE_LIST, f"{DOWNLOAD_PATH}/{PREFIX}",
                               connect_to_postgres())
