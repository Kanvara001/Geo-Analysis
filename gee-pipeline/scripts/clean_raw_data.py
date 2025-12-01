import pandas as pd
import glob
import os

RAW_CSV = "gee-pipeline/raw_export/*.csv"
RAW_PARQUET = "gee-pipeline/outputs/raw_parquet/*.parquet"
OUT_FILE = "gee-pipeline/processed/monthly_clean.csv"

files = glob.glob(RAW_CSV)

# If no CSV → read parquet and convert
if not files:
    parquet_files = glob.glob(RAW_PARQUET)
    if not parquet_files:
        print("❌ No raw CSV or Parquet files found.")
        exit(1)

    print("ℹ No CSV found. Converting Parquet → CSV automatically…")

    frames = []
    for pq in parquet_files:
        df = pd.read_parquet(pq)
        frames.append(df)

    full = pd.concat(frames)
    full.to_csv("gee-pipeline/raw_export/auto_from_parquet.csv", index=False)
    files = [ "gee-pipeline/raw_export/auto_from_parquet.csv" ]

dfs = []

for f in files:
    df = pd.read_csv(f)

    # Required columns
    mapping = {
        "Province": "province",
        "District": "amphoe",
        "Subdistric": "tambon",
    }

    for old, new in mapping.items():
        if old in df.columns:
            df[new] = df[old]

    missing = [c for c in ["province","amphoe","tambon"] if c not in df.columns]
    if missing:
        print("❌ Missing columns:", missing)
        exit(1)

    # value/count fix
    if "mean" in df.columns:
        df["value"] = pd.to_numeric(df["mean"], errors="coerce").fillna(0)
    if "count" in df.columns:
        df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)

    dfs.append(df)

full = pd.concat(dfs)
full = full.sort_values(["province","amphoe","tambon","year","month"])

os.makedirs("gee-pipeline/processed", exist_ok=True)
full.to_csv(OUT_FILE, index=False)

print("✅ Cleaned data saved:", OUT_FILE)
