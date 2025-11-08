# etl/etl_local.py
import pandas as pd
from pathlib import Path
import glob
import sqlite3

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def load_raw_files():
    files = sorted(glob.glob(str(RAW_DIR / "sales_*.csv")))
    print("Found files:", files)
    dfs = [pd.read_csv(f, parse_dates=["order_timestamp"]) for f in files]
    return pd.concat(dfs, ignore_index=True)

def clean_transform(df: pd.DataFrame):
    # basic cleaning
    df = df.dropna(subset=["order_id", "order_timestamp", "total_price"])
    df["region"] = df["region"].str.strip().str.title()
    df["category"] = df["category"].str.strip().str.title()
    # ensure numeric types
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["total_price"] = df["total_price"].astype(float)
    df["order_date"] = df["order_timestamp"].dt.date
    return df

def create_aggregates(df: pd.DataFrame):
    # daily aggregates by region & category
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

def save_locally(daily_region, daily_category):
    daily_region.to_csv(PROCESSED_DIR / "daily_region.csv", index=False)
    daily_category.to_csv(PROCESSED_DIR / "daily_category.csv", index=False)
    # also write to a small sqlite to simulate a warehouse
    conn = sqlite3.connect("data/warehouse/dev_warehouse.db")
    daily_region.to_sql("daily_region", conn, if_exists="replace", index=False)
    daily_category.to_sql("daily_category", conn, if_exists="replace", index=False)
    conn.close()
    print("Saved processed files and sqlite warehouse.")

def run():
    df = load_raw_files()
    df = clean_transform(df)
    daily_region, daily_category = create_aggregates(df)
    save_locally(daily_region, daily_category)

if __name__ == "__main__":
    run()