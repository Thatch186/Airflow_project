from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

# Define DAG arguments
default_args = {
    'owner': 'dummy_owner',
    'start_date': datetime.now(),
    'email': ['dummy_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)

# Define the task to call the shell script
extract_transform_load = BashOperator(
    task_id='extract_transform_load',
    bash_command='etl.sh',
    dag=dag,
)

extract_transform_load
