from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'peerapon.li@yipintsoi.com',  # Set the email(s) for this DAG
    'email_on_failure': True,              # Enable email on failure
    'email_on_retry': False,               # Enable email on retry (optional)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def sample_task():
    # Your task logic here
    print("Executing task...")

# Create the DAG
with DAG(
    'example_dag_with_email_alert',
    default_args=default_args,
    description='DAG with email alert on failure',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024,11,6),
    catchup=False,
) as dag:

    # Define task
    task = PythonOperator(
        task_id='sample_task',
        python_callable=sample_task
    )