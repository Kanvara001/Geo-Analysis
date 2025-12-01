import pandas as pd
import glob
import os

RAW_DIR = "gee-pipeline/outputs/raw_parquet/*.parquet"
OUT_FILE = "gee-pipeline/processed/monthly_clean.csv"

files = glob.glob(RAW_DIR)
if not files:
    print("❌ No parquet files found in outputs/raw_parquet/")
    exit(1)

dfs = []
for f in files:
    df = pd.read_parquet(f)

    missing = [c for c in ["Province","District","Subdistric"] if c not in df.columns]
    if missing:
        print("❌ Missing columns in:", f, missing)
        exit(1)

    # extract year-month from filename
    base = os.path.basename(f).replace(".parquet","")
    parts = base.split("_")

    variable = parts[0]
    year = int(parts[1])
    month = int(parts[2])

    df["variable"] = variable
    df["year"] = year
    df["month"] = month

    # convert names to your standard
    df = df.rename(columns={
        "Province": "province",
        "District": "amphoe",
        "Subdistric": "tambon"
    })

    # value field
    if "mean" in df.columns:
        df["value"] = pd.to_numeric(df["mean"], errors="coerce")
    elif "count" in df.columns:
        df["value"] = pd.to_numeric(df["count"], errors="coerce")
    else:
        df["value"] = None

    dfs.append(df)

full = pd.concat(dfs)
full = full.sort_values(["province","amphoe","tambon","year","month"])

os.makedirs("gee-pipeline/processed", exist_ok=True)
full.to_csv(OUT_FILE, index=False)

print("✅ Cleaned data saved:", OUT_FILE)
