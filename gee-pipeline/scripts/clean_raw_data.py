import pandas as pd
import glob
import os

RAW_DIR = "gee-pipeline/outputs/raw_parquet/*.parquet"
OUT_FILE = "gee-pipeline/processed/monthly_clean.csv"

files = glob.glob(RAW_DIR)
if not files:
    print("❌ No Parquet files found in outputs/raw_parquet/")
    exit(1)

dfs = []
for f in files:
    df = pd.read_parquet(f)

    # Check required fields exist
    missing = [c for c in ["Province", "District", "Subdistric"] if c not in df.columns]
    if missing:
        print("❌ Missing columns in:", f, missing)
        exit(1)

    # Extract year_month from filename
    base = os.path.basename(f).replace(".parquet", "")
    try:
        variable, year, month = base.split("_")
        df["year"] = int(year)
        df["month"] = int(month)
        df["variable"] = variable
    except:
        print("⚠ Filename format incorrect:", f)

    # Rename for consistency
    df = df.rename(columns={
        "Province": "province",
        "District": "amphoe",
        "Subdistric": "tambon"
    })

    # Convert numeric
    df["value"] = pd.to_numeric(df.get("mean"), errors="coerce")
    df["count"] = pd.to_numeric(df.get("count"), errors="coerce")

    dfs.append(df)

full = pd.concat(dfs)
full = full.sort_values(["province", "amphoe", "tambon", "year", "month"])

# Export cleaned
os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
full.to_csv(OUT_FILE, index=False)

print("✅ Cleaned monthly dataset saved:", OUT_FILE)
