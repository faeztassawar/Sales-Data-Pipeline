from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'faez',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sales_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    run_etl = BashOperator(
        task_id='run_etl_s3_snowflake',
        bash_command=(
            # activate venv is not used inside container; container runs python directly
            'python /opt/airflow/etl/etl_s3_snowflake.py'
        ),
        env={
            # Ensure env available inside container — container will read airflow_env/.env if you mounted it
            # Alternatively you can set specific env vars here, but copying .env to airflow_env is easier.
        }
    )

    run_etl