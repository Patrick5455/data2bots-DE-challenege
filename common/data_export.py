import csv

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from utils import *
from ddl import crate_staging_tables


class ExtractAndLoad:
    """
        This class downloads data from a public s3 bucket and loads to a postgres DB
    """

    def __init__(self,
                 s3_config: S3Config,
                 db_config: DBConfig,
                 db_connection: connection = None
                 ):

        if db_connection is None:
            raise Exception("database connecting is none")

        self.__db_connection = db_connection
        self.__s3_config = s3_config
        self.__db_config = db_config

    def _download_from_s3(self):
        try:
            s3 = boto3.client('s3',
                              region_name=self.__s3_config.s3_region,
                              config=Config(signature_version=UNSIGNED))
            response = s3.list_objects(Bucket=self.__s3_config.s3_bucket_name,
                                       Prefix=self.__s3_config.prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    object_key = obj['Key']
                    key = object_key.split("/")[-1]
                    download_path = f'{self.__s3_config.download_path}' \
                                    f'/{object_key.split("/")[0]}'
                    if key in self.__s3_config.file_list:
                        download_file_location = f"{download_path}/{key}"
                        print(f"downloading {key} in {download_file_location}.")
                        os.makedirs(
                            os.path.dirname(
                                download_file_location), exist_ok=True)
                        s3.download_file(self.__s3_config.s3_bucket_name,
                                         object_key,
                                         download_file_location)

        except Exception as e:
            print(f"Error while exporting data from s3: {e}")

    def _create_staging_tables_and_load_data_from_csv(self, csv_files_location):
        cursor = self.__db_connection.cursor()
        schema_name = self.__db_config.db_schema
        csv_files = [f for f in os.listdir(csv_files_location) if f.endswith('.csv')]
        for file in csv_files:
            table_name = os.path.splitext(file)[0]  # Use the file name as the table name
            file_path = os.path.join(csv_files_location, file)
            with open(file_path, 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                header = next(csv_reader)  # remove header row from input reader object
                print(f"creating table {schema_name}.{table_name} ")
                cursor.execute(crate_staging_tables(schema_name=schema_name,
                                                    table_name=table_name))
                self.__db_connection.commit()

    def _load_csv_files_to_postgres(self):
        try:
            cur = self.__db_connection.cursor()
            parent_path = f"{self.__s3_config.download_path}/{self.__s3_config.prefix}"
            for file_name in self.__s3_config.file_list:
                table_name = f'{self.__db_config.db_schema}.{file_name.split(".")[0]}'
                fully_qualified_file_name = f"{parent_path}/{file_name}"
                print(f"loading data from {fully_qualified_file_name}"
                      f" file to table {table_name}")
                truncate_sql = f"TRUNCATE TABLE {table_name}"
                copy_sql = f"COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER"
                cur.execute(truncate_sql)
                with open(fully_qualified_file_name, 'r') as f:
                    next(f)
                    cur.copy_expert(sql=copy_sql, file=f)

            self.__db_connection.commit()
            cur.close()
        except Exception as e:
            self.__db_connection.close()
            print(f"Error while loading data to postgres: {e}")

    def export_data(self):
        self._download_from_s3()
        self._create_staging_tables_and_load_data_from_csv(
            f"{self.__s3_config.download_path}/{self.__s3_config.prefix}")
        self._load_csv_files_to_postgres()
        self.__db_connection.close()


if __name__ == '__main__':
    s3_config = S3Config(yaml_configs_loader("../configs.yaml"))
    db_config = DBConfig(load_db_configs_in_dict())
    extract_and_load = ExtractAndLoad(
        s3_config=s3_config,
        db_config=db_config,
        db_connection=connect_to_postgres(db_config)
    )

    extract_and_load.export_data()
