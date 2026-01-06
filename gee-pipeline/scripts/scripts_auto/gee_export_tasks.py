import os
import ee
import pandas as pd
from datetime import datetime, timedelta
import argparse

assert "scripts_auto" in __file__, "❌ Wrong script path"

MERGED_PATH = "gee-pipeline/outputs/merged/merged_dataset.parquet"

def get_last_date():
    if not os.path.exists(MERGED_PATH):
        return datetime(2015, 1, 1)

    df = pd.read_parquet(MERGED_PATH)
    df["date"] = pd.to_datetime(df["date"])
    return df["date"].max()

def main(var):
    ee.Initialize()

    last_date = get_last_date()
    start_date = last_date + timedelta(days=1)
    end_date = datetime.today()

    if start_date >= end_date:
        print("✅ No new data to export")
        return

    print(f"🚀 AUTO EXPORT {var}")
    print(f"📅 {start_date.date()} → {end_date.date()}")

    # 👉 ใส่ export logic เดิมของคุณตรงนี้
    # ใช้ start_date / end_date แทน hardcode ปี

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--var", required=True)
    args = parser.parse_args()

    main(args.var)
