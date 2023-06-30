from common.utils import *
from pipeline.batch.data_export import ExtractAndLoad
from pipeline.batch.data_transformation import TransformAndDump

if __name__ == '__main__':
    s3_config_obj = S3Config(yaml_configs_loader("configs.yaml",
                                                 parent_level='S3_CONFIG'))
    db_config_obj = DBConfig(load_db_configs_in_dict())
    warehouse_config_obj = WarehouseConfig(yaml_configs_loader("configs.yaml",
                                                               parent_level='WAREHOUSE_CONFIG'))
    extract_and_load = ExtractAndLoad(
        s3_config=s3_config_obj,
        db_config=db_config_obj,
        db_connection=connect_to_postgres(db_config_obj),
        warehouse_config=warehouse_config_obj
    )
    extract_and_load.export_and_load_job()

    transform_and_dump = TransformAndDump(
        s3_config=s3_config_obj,
        db_config=db_config_obj,
        warehouse_config=warehouse_config_obj,
        db_connection=connect_to_postgres(db_config_obj)
    )

    transform_and_dump.transform_and_dump()
