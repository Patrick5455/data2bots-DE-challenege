import psycopg2
from psycopg2._psycopg import connection
from constants import *
import yaml


class S3Config:
    def __init__(self, s3_config_dict: dict):
        self._s3_bucket_name = s3_config_dict["BUCKET_NAME"]
        self._prefix = s3_config_dict["PREFIX"]
        self._file_list = s3_config_dict["FILE_LIST"]
        self._download_path = s3_config_dict["DOWNLOAD_PATH"]
        self._s3_region = s3_config_dict["S3_REGION"]

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


class DBConfig:
    def __init__(self, config_dict):
        self._db_host = config_dict.get('DB_HOST')
        self._db_port = config_dict.get('DB_PORT')
        self._db_name = config_dict.get('DB_NAME')
        self._db_schemas = config_dict.get('DB_SCHEMAS')
        self._db_user = config_dict.get('DB_USER')
        self._db_password = config_dict.get('DB_PASSWORD')

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
    def db_schemas(self):
        return self._db_schemas

    @db_schemas.setter
    def db_schemas(self, value):
        self._db_schemas = value

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


def yaml_configs_loader(config_file: str) -> dict:
    with open(config_file, "r") as file:
        config_dict = yaml.safe_load(file)
    return config_dict


def connect_to_postgres() -> connection:
    try:
        data2bots_db_connection = psycopg2.connect(host=DB_HOST, port=DB_PORT,
                                                   database=DB_NAME, user=DB_USER,
                                                   password=DB_PASSWORD)
        print('connected to DB successfully')
        return data2bots_db_connection
    except Exception as db_error:
        print(f"something went wrong while trying to connect with "
              f"{DB_NAME} DB", str(db_error))
