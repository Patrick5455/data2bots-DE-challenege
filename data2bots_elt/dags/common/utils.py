import os

import psycopg2
import yaml
from psycopg2._psycopg import connection

from dotenv import load_dotenv

load_dotenv()


class S3Config:
    def __init__(self, s3_config_dict: dict = None):
        if s3_config_dict is None or len(s3_config_dict) == 0:
            raise Exception("s3 configs map passed is none or empty")
        else:
            print("S3 configs map successfully loaded")
        if 'S3_CONFIG' in s3_config_dict:
            s3_config = s3_config_dict.get('S3_CONFIG')
            self._s3_bucket_name = s3_config.get('BUCKET_NAME')
            self._prefix = s3_config.get('PREFIX')
            self._file_list = s3_config.get('FILE_LIST')
            self._download_path = s3_config.get('DOWNLOAD_PATH')
            self._s3_region = s3_config.get('S3_REGION')
            self._export_location = s3_config.get('EXPORT_LOCATION')
        else:
            self._s3_bucket_name = s3_config_dict.get('BUCKET_NAME')
            self._prefix = s3_config_dict.get('PREFIX')
            self._file_list = s3_config_dict.get('FILE_LIST')
            self._download_path = s3_config_dict.get('DOWNLOAD_PATH')
            self._s3_region = s3_config_dict.get('S3_REGION')
            self._export_location = s3_config_dict.get('EXPORT_LOCATION')

    @property
    def s3_bucket_name(self):
        return self._s3_bucket_name

    @s3_bucket_name.setter
    def s3_bucket_name(self, value):
        self._s3_bucket_name = value

    @property
    def prefix(self):
        return self._prefix

    @prefix.setter
    def prefix(self, value):
        self._prefix = value

    @property
    def file_list(self):
        return self._file_list

    @file_list.setter
    def file_list(self, value):
        self._file_list = value

    @property
    def download_path(self):
        return self._download_path

    @download_path.setter
    def download_path(self, value):
        self._download_path = value

    @property
    def s3_region(self):
        return self._s3_region

    @s3_region.setter
    def s3_region(self, value):
        self._s3_region = value

    @property
    def export_location(self):
        return self._export_location

    @export_location.setter
    def export_location(self, value):
        self._export_location = value


class WarehouseConfig:
    def __init__(self, warehouse_config_dict: dict = None):
        if warehouse_config_dict is None or len(warehouse_config_dict) == 0:
            raise Exception("warehouse configs map passed is none or empty")
        else:
            print("warehouse configs map successfully loaded")
        if 'WAREHOUSE_CONFIG' in warehouse_config_dict:
            warehouse_config = warehouse_config_dict.get('WAREHOUSE_CONFIG')
            self._staging_tables = warehouse_config.get('STAGING_TABLES')
            self._analytics_tables = warehouse_config.get('ANALYTICS_TABLES')
        else:
            self._staging_tables = warehouse_config_dict.get('STAGING_TABLES')
            self._analytics_tables = warehouse_config_dict.get('ANALYTICS_TABLES')

    @property
    def staging_tables(self):
        return self._staging_tables

    @staging_tables.setter
    def staging_tables(self, value):
        self._staging_tables = value

    @property
    def analytics_tables(self):
        return self._analytics_tables

    @analytics_tables.setter
    def analytics_tables(self, value):
        self._analytics_tables = value


class DBConfig:
    def __init__(self, db_config_dict):
        if db_config_dict is None or len(db_config_dict) == 0:
            raise Exception("DB configs map passed is none or empty")
        else:
            print("DB configs map successfully loaded")
        self._db_host = db_config_dict.get('DB_HOST')
        self._db_port = db_config_dict.get('DB_PORT')
        self._db_name = db_config_dict.get('DB_NAME')
        self._staging_db_schema = db_config_dict.get('STAGING_DB_SCHEMA')
        self._analytics_db_schema = db_config_dict.get('ANALYTICS_DB_SCHEMA')
        self._db_user = db_config_dict.get('DB_USER')
        self._db_password = db_config_dict.get('DB_PASSWORD')

    @property
    def db_host(self):
        return self._db_host

    @db_host.setter
    def db_host(self, value):
        self._db_host = value

    @property
    def db_port(self):
        return self._db_port

    @db_port.setter
    def db_port(self, value):
        self._db_port = value

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, value):
        self._db_name = value

    @property
    def staging_db_schema(self):
        return self._staging_db_schema

    @staging_db_schema.setter
    def staging_db_schema(self, value):
        self._staging_db_schema = value

    @property
    def analytics_db_schema(self):
        return self._analytics_db_schema

    @analytics_db_schema.setter
    def analytics_db_schema(self, value):
        self._analytics_db_schema = value

    @property
    def db_user(self):
        return self._db_user

    @db_user.setter
    def db_user(self, value):
        self._db_user = value

    @property
    def db_password(self):
        return self._db_password

    @db_password.setter
    def db_password(self, value):
        self._db_password = value


def yaml_configs_loader(config_file: str, parent_level: str = None) -> dict:
    print(f"config_file: {config_file}")
    with open(config_file, "r") as file:
        config_dict = yaml.safe_load(file)
    if parent_level is None:
        print("successfully loaded yaml configs file into a map")
        return config_dict
    else:
        print(f"successfully loaded yaml configs file at parent level {parent_level} into a map")
        return config_dict[parent_level]


def load_db_configs_in_dict() -> dict:
    config_dict = {
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_PORT': os.getenv('DB_PORT'),
        'DB_NAME': os.getenv('DB_NAME'),
        'STAGING_DB_SCHEMA': os.getenv('STAGING_DB_SCHEMA'),
        'ANALYTICS_DB_SCHEMA': os.getenv('ANALYTICS_DB_SCHEMA'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD')
    }
    print("successfully loaded db configs into a map")
    return config_dict


def connect_to_postgres(db_config: DBConfig) -> connection:
    try:
        data2bots_db_connection = psycopg2.connect(host=db_config.db_host,
                                                   port=db_config.db_port,
                                                   database=db_config.db_name,
                                                   user=db_config.db_user,
                                                   password=db_config.db_password)
        print('connected to DB successfully')
        return data2bots_db_connection
    except Exception as db_error:
        print(f"something went wrong while trying to connect with "
              f"{db_config.db_name} DB", str(db_error))
