# etl/etl_s3_snowflake.py
import os
from pathlib import Path
import glob
import pandas as pd
import sqlite3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

USE_S3 = os.getenv("USE_S3", "false").lower() == "true"
USE_SNOWFLAKE = os.getenv("USE_SNOWFLAKE", "false").lower() == "true"

# AWS / S3
import boto3
from botocore.exceptions import ClientError

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")
S3_PREFIX = os.getenv("S3_PREFIX", "data/raw/")  # optional prefix in bucket

# Snowflake
SNOW_USER = os.getenv("SNOWFLAKE_USER")
SNOW_PWD = os.getenv("SNOWFLAKE_PASSWORD")
SNOW_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOW_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOW_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOW_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
WAREHOUSE_DB_PATH = Path("data/warehouse/dev_warehouse.db")
WAREHOUSE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------
# File listing / S3 helpers
# ---------------------
def list_local_files():
    return sorted(glob.glob(str(RAW_DIR / "sales_*.csv")))

def list_s3_files():
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET_NAME is not set in env.")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv") and "sales_" in key:
                keys.append(key)
    return sorted(keys)

def download_s3_to_local(key, local_dir=RAW_DIR):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    local_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(local_dir) / Path(key).name
    s3.download_file(S3_BUCKET, key, str(filename))
    return str(filename)

# ---------------------
# ETL logic
# ---------------------
def load_raw_files():
    if USE_S3:
        print("Listing CSVs from S3...")
        keys = list_s3_files()
        print("Found S3 keys:", keys)
        local_files = []
        for k in keys:
            fn = download_s3_to_local(k)
            local_files.append(fn)
        dfs = [pd.read_csv(f, parse_dates=["order_timestamp"]) for f in local_files]
    else:
        files = list_local_files()
        print("Found files:", files)
        dfs = [pd.read_csv(f, parse_dates=["order_timestamp"]) for f in files]
    return pd.concat(dfs, ignore_index=True)

def clean_transform(df: pd.DataFrame):
    df = df.dropna(subset=["order_id", "order_timestamp", "total_price"])
    df["region"] = df["region"].astype(str).str.strip().str.title()
    df["category"] = df["category"].astype(str).str.strip().str.title()
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["total_price"] = df["total_price"].astype(float)
    df["order_date"] = pd.to_datetime(df["order_timestamp"]).dt.date
    return df

def create_aggregates(df: pd.DataFrame):
    daily_region = df.groupby(["order_date", "region"]).agg(
        orders=("order_id", "nunique"),
        total_revenue=("total_price", "sum"),
        total_qty=("quantity", "sum")
    ).reset_index()
    daily_category = df.groupby(["order_date", "category"]).agg(
        orders=("order_id", "nunique"),
        total_revenue=("total_price", "sum"),
        total_qty=("quantity", "sum")
    ).reset_index()
    return daily_region, daily_category

# ---------------------
# Data quality checks
# ---------------------
def run_data_quality_checks(df: pd.DataFrame, name: str, min_rows: int = 1, max_null_pct: float = 0.10):
    """
    Basic DQ checks:
      - row count >= min_rows
      - no column has more than max_null_pct nulls
    Raises Exception on failure.
    """
    rows = len(df)
    if rows < min_rows:
        raise ValueError(f"Data quality failed for {name}: row count {rows} < min_rows {min_rows}")
    null_pct = df.isna().mean()
    bad_cols = [col for col, pct in null_pct.items() if pct > max_null_pct]
    if bad_cols:
        raise ValueError(f"Data quality failed for {name}: columns with high null pct > {max_null_pct}: {bad_cols}")
    print(f"✅ DQ passed for {name}: rows={rows}, max_null_pct={null_pct.max():.3f}")

# ---------------------
# Snowflake helper (optional)
# ---------------------
def load_to_snowflake(df: pd.DataFrame, table_name: str):
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
    except Exception as e:
        raise RuntimeError("Snowflake connector not installed or not usable: " + str(e))

    if not all([SNOW_USER, SNOW_PWD, SNOW_ACCOUNT, SNOW_WAREHOUSE, SNOW_DATABASE]):
        raise ValueError("Snowflake credentials/params missing in env")

    ctx = snowflake.connector.connect(
        user=SNOW_USER,
        password=SNOW_PWD,
        account=SNOW_ACCOUNT,
        warehouse=SNOW_WAREHOUSE,
        database=SNOW_DATABASE,
        schema=SNOW_SCHEMA,
        client_session_keep_alive=True
    )
    try:
        success, nchunks, nrows, _ = write_pandas(ctx, df, table_name.upper())
        print(f"Snowflake load success={success}, rows={nrows}")
    finally:
        ctx.close()

# ---------------------
# Local save fallback
# ---------------------
def save_to_sqlite(daily_region, daily_category):
    daily_region.to_csv(PROCESSED_DIR / "daily_region.csv", index=False)
    daily_category.to_csv(PROCESSED_DIR / "daily_category.csv", index=False)
    conn = sqlite3.connect(str(WAREHOUSE_DB_PATH))
    try:
        daily_region.to_sql("daily_region", conn, if_exists="replace", index=False)
        daily_category.to_sql("daily_category", conn, if_exists="replace", index=False)
    finally:
        conn.close()
    print("Saved processed files and sqlite warehouse at:", WAREHOUSE_DB_PATH)

# ---------------------
# Main run
# ---------------------
def run():
    df = load_raw_files()
    df = clean_transform(df)

    # data quality on raw (light)
    try:
        run_data_quality_checks(df, "raw_orders", min_rows=10, max_null_pct=0.15)
    except Exception as e:
        print("DQ failed on raw data:", e)
        raise

    daily_region, daily_category = create_aggregates(df)

    # data quality on aggregates
    try:
        run_data_quality_checks(daily_region, "daily_region", min_rows=1, max_null_pct=0.5)
        run_data_quality_checks(daily_category, "daily_category", min_rows=1, max_null_pct=0.5)
    except Exception as e:
        print("DQ failed on aggregates:", e)
        raise

    # Save processed CSVs locally always
    daily_region.to_csv(PROCESSED_DIR / "daily_region.csv", index=False)
    daily_category.to_csv(PROCESSED_DIR / "daily_category.csv", index=False)

    if USE_SNOWFLAKE:
        try:
            print("Attempting to load daily_region -> Snowflake...")
            load_to_snowflake(daily_region, "daily_region")
            print("Attempting to load daily_category -> Snowflake...")
            load_to_snowflake(daily_category, "daily_category")
        except Exception as e:
            print("Snowflake load failed:", e)
            print("Falling back to local sqlite storage.")
            save_to_sqlite(daily_region, daily_category)
    else:
        save_to_sqlite(daily_region, daily_category)

if __name__ == "__main__":
    run()