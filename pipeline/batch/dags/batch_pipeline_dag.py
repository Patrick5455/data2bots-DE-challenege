from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from transformations.python.transform_functions import transform_data
from python.data_ingestion import ingest_data
from python.data_export import export_data

# Define your DAG
dag = DAG(
    'batch_pipeline_dag',
    description='Batch Data Pipeline',
    schedule_interval='0 0 * * *',  # Run once daily at midnight
    start_date=datetime(2023, 6, 1),
    catchup=False
)

# Define the tasks
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

export_task = PythonOperator(
    task_id='export_data',
    python_callable=export_data,
    dag=dag
)

# Set the task dependencies
ingest_task >> transform_task >> export_task
