import csv
import io

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from dags.common.dml import create_analytics_tables
from dags.common.utils import *


class TransformAndDump:
    """
    This class does SQL transformation/analysis on the data within the postgres data warehouse
    and the result saved in a new analytics schema and dumped back to our datalake on s3
    """

    def __init__(self,
                 s3_config: S3Config,
                 db_config: DBConfig,
                 warehouse_config: WarehouseConfig,
                 db_connection: connection = None
                 ):

        if db_connection is None:
            raise Exception("database connecting is none")

        self.__s3_config = s3_config
        self.__db_config = db_config
        self.__warehouse_config = warehouse_config
        self.__db_connection = db_connection

    def _write_query_result_to_s3(self, key: str, data = None):
        try:
            s3 = boto3.client('s3',
                              region_name=self.__s3_config.s3_region,
                              config=Config(signature_version=UNSIGNED))
            if data is not None:
                csv_data = io.StringIO()
                csv_writer = csv.writer(csv_data)
                csv_writer.writerows(data)
                s3.put_object(Body=csv_data.getvalue(),
                              Bucket=self.__s3_config.s3_bucket_name,
                              Key=key)
                print(f"successfully stored object to {key} in s3")
            else:
                print("no data to write to s3 bucket")
        except Exception as e:
            print(f"error while exporting data to datalake: {e}")

    def _transform_tables_for_analytics_schema(self):
        try:
            cur = self.__db_connection.cursor()
            analytics_schema_name = self.__db_config.analytics_db_schema
            staging_schema_name = self.__db_config.staging_db_schema
            for table_name in self.__warehouse_config.analytics_tables:
                print(f"running analytics query for {analytics_schema_name}.{table_name}")
                cur.execute(create_analytics_tables(staging_schema_name=staging_schema_name,
                                                    analysis_schema_name=analytics_schema_name,
                                                    analysis_name=table_name))
                print(f"query ran successfully for  {analytics_schema_name}.{table_name}")
                self.__db_connection.commit()
                key = '{file_location}/{file_name}'.format(
                    file_location=self.__s3_config.export_location,
                    file_name=f"{table_name}.csv")
                fetch_data_query = f"SELECT * FROM {analytics_schema_name}.{table_name}"
                cur.execute(fetch_data_query)
                self._write_query_result_to_s3(data=cur.fetchall(), key=key)
                print(f"query result successfully written to {key}")
            cur.close()
        except Exception as e:
            self.__db_connection.close()
            print(f"error while running analytics queries in warehouse: {e}")

    def transform_and_dump(self):
        self._transform_tables_for_analytics_schema()
        self.__db_connection.close()
