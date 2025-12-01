import pandas as pd
import glob
import os

RAW_CSV = "gee-pipeline/raw_export/*.csv"
RAW_PARQUET = "gee-pipeline/outputs/raw_parquet/*.parquet"
OUT_FILE = "gee-pipeline/processed/monthly_clean.csv"

os.makedirs("gee-pipeline/raw_export", exist_ok=True)
os.makedirs("gee-pipeline/processed", exist_ok=True)

csv_files = glob.glob(RAW_CSV)

# -------------------------------------------------------
# If no CSV found → generate auto-CSV from Parquet
# -------------------------------------------------------
if not csv_files:
    print("ℹ No CSV found. Converting Parquet → CSV automatically…")

    pq_files = glob.glob(RAW_PARQUET)
    if len(pq_files) == 0:
        print("❌ No data found in outputs/raw_parquet/")
        exit(1)

    all_df = []

    for f in pq_files:
        df = pd.read_parquet(f)

        # Extract variable + date from filename
        base = os.path.basename(f).replace(".parquet", "")
        variable, year, month = base.split("_")
        df["variable"] = variable
        df["year"] = int(year)
        df["month"] = int(month)

        all_df.append(df)

    full = pd.concat(all_df, ignore_index=True)

    # Save auto CSV
    auto_csv = "gee-pipeline/raw_export/auto_from_parquet.csv"
    full.to_csv(auto_csv, index=False)
    print("✔ Auto-created:", auto_csv)

    csv_files = [auto_csv]


# -------------------------------------------------------
# Clean CSV files
# -------------------------------------------------------
dfs = []
for f in csv_files:
    df = pd.read_csv(f)

    # Fix shapefile columns
    rename_map = {
        "Province": "province",
        "District": "amphoe",
        "Subdistric": "tambon",
    }
    df = df.rename(columns=rename_map)

    missing = [c for c in ["province", "amphoe", "tambon"] if c not in df.columns]
    if missing:
        print("❌ Missing required columns:", missing)
        exit(1)

    df["value"] = pd.to_numeric(df.get("mean", df.get("value", 0)), errors="coerce").fillna(0)

    dfs.append(df)

full = pd.concat(dfs, ignore_index=True)
full.to_csv(OUT_FILE, index=False)

print("✅ Cleaned data saved →", OUT_FILE)
