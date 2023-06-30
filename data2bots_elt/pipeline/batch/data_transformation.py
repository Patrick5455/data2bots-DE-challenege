from common.utils import *
from common.dml import create_analytics_tables


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

    def transform_tables_for_analytics_schema(self):
        try:
            cursor = self.__db_connection.cursor()
            analytics_schema_name = self.__db_config.analytics_db_schema
            staging_schema_name = self.__db_config.staging_db_schema
            for table_name in self.__warehouse_config.analytics_tables:
                print(f"running analytics query for {analytics_schema_name}.{table_name}")
                cursor.execute(create_analytics_tables(staging_schema_name=staging_schema_name,
                                                       analysis_schema_name=analytics_schema_name,
                                                       analysis_name=table_name))
                print(f"query ran successfully for  {analytics_schema_name}.{table_name}")
                self.__db_connection.commit()
        except Exception as e:
            self.__db_connection.close()
            print(f"error while running analytics queries in warehouse: {e}")

    def transform_and_dump(self):
        self.transform_tables_for_analytics_schema()
        self.__db_connection.close()

