from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from transformations.python.transform_functions import transform_data
from python.kafka_consumer import consume_data

# Define your DAG
dag = DAG(
    'streaming_pipeline_dag',
    description='Streaming Data Pipeline',
    schedule_interval=None,  # Set to None for streaming pipeline
    start_date=datetime(2023, 6, 1),
    catchup=False
)

# Define the tasks
consume_task = PythonOperator(
    task_id='consume_data',
    python_callable=consume_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# Set the task dependencies
consume_task >> transform_task
