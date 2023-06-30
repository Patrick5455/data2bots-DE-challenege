import sys

# sys.path.append('common')
# sys.path.append('pipeline')
# sys.path.append('../dbt-dags')
from airflow.operators.empty import EmptyOperator

print(f"sys.modules:{sys.modules}")
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.utils import *
from pipeline.batch.data_export import ExtractAndLoad
from pipeline.batch.data_transformation import TransformAndDump

load_dotenv()


dag_file_path = os.path.abspath(__file__)
dag_directory = os.path.dirname(dag_file_path)
YAML_CONFIG_PATH = os.path.join(dag_directory, 'configs.yaml')

s3_config_obj = S3Config(yaml_configs_loader(config_file=YAML_CONFIG_PATH,
                                             parent_level='S3_CONFIG'))
db_config_obj = DBConfig(load_db_configs_in_dict())
warehouse_config_obj = WarehouseConfig(yaml_configs_loader(config_file=YAML_CONFIG_PATH,
                                                           parent_level='WAREHOUSE_CONFIG'))


def export_and_load_job():
    extract_and_load_obj = ExtractAndLoad(
        s3_config=s3_config_obj,
        db_config=db_config_obj,
        db_connection=connect_to_postgres(db_config_obj),
        warehouse_config=warehouse_config_obj
    )
    extract_and_load_obj.export_and_load_job()


def transform_and_dump():
    transform_and_dump_obj = TransformAndDump(
        s3_config=s3_config_obj,
        db_config=db_config_obj,
        warehouse_config=warehouse_config_obj,
        db_connection=connect_to_postgres(db_config_obj)
    )
    transform_and_dump_obj.transform_and_dump()


default_args = {
    'owner': 'patrojun6040',
    'start_date': datetime(2023, 6, 30),
}

dag = DAG(
    'data2bots_elt_pipeline',  # Specify a unique DAG ID
    default_args=default_args,
    schedule_interval='0 0 * * *'  # run daily at midnight)
)

task_export_and_load = PythonOperator(
    task_id='export_and_load',
    python_callable=export_and_load_job,
    dag=dag
)

task_transform_and_dump = PythonOperator(
    task_id='transform_and_dump',
    python_callable=transform_and_dump,
    dag=dag
)

alert_on_finish = EmptyOperator(
    task_id='send_email_and_slack_alert',
    dag=dag
)


task_transform_and_dump.set_downstream(alert_on_finish)
task_export_and_load.set_downstream(task_transform_and_dump)
