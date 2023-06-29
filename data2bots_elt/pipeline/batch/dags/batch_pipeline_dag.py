from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

# Define the DAG
dag = DAG(
    'nightly_pipeline_dag',
    start_date=datetime(2023, 6, 25),
    schedule_interval='0 0 * * *'  # Run every day at midnight
)

# Define the tasks
# Task to download the CSV files from S3
download_csv_files = PythonOperator(
    task_id='download_csv_files',
    source_s3_key='d2b-internal-assessment-bucket/orders_data/*',
    dest_local_directory='/path/to/dbt-dags/data2bots-DE-challenge/data',
    aws_conn_id='aws_default',  # Connection ID for AWS credentials
    dag=dag
)

# Task to run the DBT stages
run_dbt_stages = BashOperator(
    task_id='run_dbt_stages',
    bash_command='cd /path/to/dbt-dags && dbt run',
    dag=dag
)

# Task to export tables to S3
export_tables_to_s3 = S3CreateObjectOperator(
    task_id='export_tables_to_s3',
    source_local_directory='/path/to/analytics_export',
    dest_s3_key='analytics_export/patrojun6040/',
    aws_conn_id='aws_default',  # Connection ID for AWS credentials
    dag=dag
)

# Set task dependencies
download_csv_files >> run_dbt_stages >> export_tables_to_s3
