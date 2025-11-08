"""
sales_etl_pipeline.py
Simple, clear DAG for the Sales Data Pipeline:
- Lists CSVs from S3 prefix
- Downloads each CSV locally
- Transforms with pandas (example: daily aggregates)
- Writes results to Postgres (dev warehouse) or Snowflake (if enabled)
"""

from __future__ import annotations
import os
import io
import logging
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

log = logging.getLogger(__name__)

# Config / env (reads from your .env mounted into /opt/airflow/.env or Docker env)
AWS_REGION = os.environ.get("AWS_REGION")
S3_BUCKET = os.environ.get("S3_BUCKET_NAME")
S3_RAW_DATA_PATH = os.environ.get("S3_RAW_DATA_PATH")
USE_SNOWFLAKE = os.environ.get("USE_SNOWFLAKE")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Postgres dev warehouse (the local or dev Postgres)
POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "faez",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    description="ETL: S3 -> transform -> warehouse (Postgres/Snowflake)",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "etl"],
) as dag:

    def list_s3_keys(**_kwargs) -> List[str]:
        """Return list of S3 keys under S3_RAW_DATA_PATH"""
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        paginator = s3.get_paginator("list_objects_v2")
        keys: List[str] = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_RAW_DATA_PATH):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".csv"):
                    keys.append(key)
        log.info("Found keys: %s", keys)
        return keys

    def download_and_transform(**context):
        """Download each CSV from S3, perform transformation, and return as CSV text."""
        keys = context["ti"].xcom_pull(task_ids="list_s3_keys")
        if not keys:
            log.info("No CSV keys found.")
            return None

        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        agg_frames = []
        for key in keys:
            log.info("Downloading %s", key)
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()))

            # Ensure necessary columns
            if "order_timestamp" not in df.columns:
                log.warning("Skipping file %s because 'order_timestamp' column not found.", key)
                continue

            df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], errors="coerce").dt.date
            if "region" not in df.columns:
                df["region"] = "unknown"
            if "category" not in df.columns:
                df["category"] = "unknown"

            # Daily aggregates
            daily = (
                df.groupby(["order_timestamp", "region", "category"], dropna=False)
                .agg(total_sales=("amount", "sum") if "amount" in df.columns else ("order_id", "count"))
                .reset_index()
            )
            agg_frames.append(daily)

        if not agg_frames:
            log.info("No valid dataframes after transformation.")
            return None

        result = pd.concat(agg_frames, ignore_index=True)
        buf = io.StringIO()
        result.to_csv(buf, index=False)
        csv_text = buf.getvalue()
        context["ti"].xcom_push(key="transformed_csv", value=csv_text)
        log.info("Transformation complete, rows=%d", len(result))
        return True

    def load_to_postgres(**context):
        csv_text = context["ti"].xcom_pull(key="transformed_csv", task_ids="download_and_transform")
        if not csv_text:
            log.info("No transformed CSV to load into Postgres.")
            return

        df = pd.read_csv(io.StringIO(csv_text))
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_sql = """
        CREATE TABLE IF NOT EXISTS sales_daily_agg (
            date DATE,
            region TEXT,
            category TEXT,
            total_sales DOUBLE PRECISION
        );
        """
        pg.run(create_sql)

        insert_sql = """
        INSERT INTO sales_daily_agg (date, region, category, total_sales)
        VALUES (%s, %s, %s, %s)
        """
        conn = pg.get_conn()
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute(
                insert_sql,
                (row["order_timestamp"], row["region"], row["category"], float(row.get("total_sales", 0.0))),
            )
        conn.commit()
        cur.close()
        log.info("Loaded %d rows into Postgres.", len(df))

    def load_to_snowflake(**context):
        if not USE_SNOWFLAKE:
            log.info("USE_SNOWFLAKE is false; skipping Snowflake load.")
            return
        csv_text = context["ti"].xcom_pull(key="transformed_csv", task_ids="download_and_transform")
        if not csv_text:
            log.info("No transformed CSV to load into Snowflake.")
            return

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        df = pd.read_csv(io.StringIO(csv_text))

        create_sql = """
        CREATE TABLE IF NOT EXISTS SALES_DAILY_AGG (
            date DATE,
            region TEXT,
            category TEXT,
            total_sales FLOAT
        );
        """
        hook.run(create_sql)

        for _, row in df.iterrows():
            hook.run(
                "INSERT INTO SALES_DAILY_AGG (date, region, category, total_sales) VALUES (%s, %s, %s, %s)",
                parameters=(str(row["order_timestamp"]), row["region"], row["category"], float(row.get("total_sales", 0.0))),
            )
        log.info("Loaded %d rows into Snowflake.", len(df))

    # Define tasks
    t1 = PythonOperator(task_id="list_s3_keys", python_callable=list_s3_keys)
    t2 = PythonOperator(task_id="download_and_transform", python_callable=download_and_transform)

    if USE_SNOWFLAKE:
        t3 = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_snowflake)
    else:
        t3 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    # Define dependencies
    t1 >> t2 >> t3
