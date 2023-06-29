from common.utils import *
from pipeline.batch.data_export import ExtractAndLoad

if __name__ == '__main__':
    s3_config_obj = S3Config(yaml_configs_loader("../configs.yaml"))
    db_config_obj = DBConfig(load_db_configs_in_dict())
    extract_and_load = ExtractAndLoad(
        s3_config=s3_config_obj,
        db_config=db_config_obj,
        db_connection=connect_to_postgres(db_config_obj))
    extract_and_load.export_and_load_job()
