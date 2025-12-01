import pandas as pd
import glob
import os

RAW_DIR = "gee-pipeline/raw_export/*.csv"
OUT_FILE = "gee-pipeline/processed/monthly_clean.csv"

files = glob.glob(RAW_DIR)
if not files:
    print("❌ No raw CSV files found in raw_export/")
    exit(1)

dfs = []
for f in files:
    df = pd.read_csv(f)

    year, month = os.path.basename(f).replace(".csv", "").split("_")
    df["year"] = int(year)
    df["month"] = int(month)

    df["value"] = pd.to_numeric(df["mean"], errors="coerce").fillna(0)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)

    dfs.append(df)

full = pd.concat(dfs)
full.to_csv(OUT_FILE, index=False)

print("✅ Cleaned data saved:", OUT_FILE)
