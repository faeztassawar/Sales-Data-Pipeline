from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define a simple Python function
def say_hello():
    print("🎉 Hello, Airflow is working perfectly!")

# Define default arguments
default_args = {
    'owner': 'faez',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=timedelta(minutes=5),  # runs every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example', 'test'],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello,
    )

    hello_task
