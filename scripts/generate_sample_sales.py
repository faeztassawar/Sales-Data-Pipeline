# scripts/generate_sample_sales.py
import pandas as pd
import numpy as np
from pathlib import Path
import random
from datetime import datetime, timedelta

OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_month_csv(year, month, rows=5000):
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year+1, 1, 1) - timedelta(seconds=1)
    else:
        end = datetime(year, month+1, 1) - timedelta(seconds=1)

    categories = ["Beverages", "Desserts", "Main Course", "Appetizers", "Salads"]
    regions = ["North", "South", "East", "West", "Central"]
    product_ids = [f"P{str(i).zfill(4)}" for i in range(1, 201)]

    data = []
    for i in range(rows):
        order_id = f"O{year}{str(month).zfill(2)}{str(i).zfill(6)}"
        date = random_date(start, end)
        product_id = random.choice(product_ids)
        category = random.choice(categories)
        region = random.choice(regions)
        quantity = random.randint(1, 5)
        price = round(random.uniform(2.5, 120.0), 2)
        total = round(quantity * price, 2)
        data.append([order_id, date.isoformat(), product_id, category, region, quantity, price, total])

    df = pd.DataFrame(data, columns=[
        "order_id", "order_timestamp", "product_id", "category", "region", "quantity", "unit_price", "total_price"
    ])
    filename = OUT_DIR / f"sales_{year}_{str(month).zfill(2)}.csv"
    df.to_csv(filename, index=False)
    print(f"Generated {filename} with {len(df)} rows.")

if __name__ == "__main__":
    # generate last 3 months for testing
    from datetime import datetime
    now = datetime.now()
    months = [(now.year, now.month), ((now.year if now.month>1 else now.year-1), (now.month-1 if now.month>1 else 12)),
              ((now.year if now.month>2 else now.year-1), (now.month-2 if now.month>2 else 12))]
    for y, m in months:
        generate_month_csv(y, m, rows=3000)