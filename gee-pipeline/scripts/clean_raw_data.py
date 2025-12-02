import pandas as pd
import numpy as np
import os
import glob

RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CLEAN = "gee-pipeline/outputs/clean"

os.makedirs(OUTPUT_CLEAN, exist_ok=True)

LONG_GAP_THRESHOLD = {
    "NDVI": 2,
    "LST": 2,
    "SoilMoisture": 1,
    "Rainfall": 1,
    "FireCount": 1,
}

# -------------------------------------------
# Load all parquet files (all variable folders)
# -------------------------------------------
def load_all():
    files = glob.glob(f"{RAW_PARQUET_DIR}/**/*.parquet", recursive=True)

    if len(files) == 0:
        raise ValueError("❌ No parquet files found in raw_parquet/. Pipeline earlier step failed.")

    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Failed to read {f}: {e}")

    if len(dfs) == 0:
        raise ValueError("❌ No usable parquet files loaded.")

    return pd.concat(dfs, ignore_index=True)

# -------------------------------------------
# Cleaning per variable
# -------------------------------------------
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()

    # convert value to numeric
    temp["value"] = pd.to_numeric(temp["value"], errors="coerce")

    # sort
    temp = temp.sort_values(
        ["province", "amphoe", "tambon", "year", "month"]
    )

    def fill_group(g):
        s = g["value"]

        # detect long gaps
        long_gap = (
            s.isna()
            .astype(int)
            .groupby((s.notna()).cumsum())
            .transform("count")
            .max()
        )

        if long_gap >= LONG_GAP_THRESHOLD.get(var, 2):
            climatology = s.groupby(g["month"]).transform("mean")
            new = s.fillna(climatology)
        else:
            new = s.interpolate()

        return new

    clean_values = temp.groupby(
        ["province", "amphoe", "tambon"]
    ).apply(fill_group)

    temp["clean_value"] = clean_values.values
    return temp

# -------------------------------------------
# RUN CLEANING
# -------------------------------------------
df = load_all()

cleaned = []
for var in df["variable"].unique():
    print(f"🧼 Cleaning variable → {var}")
    cleaned.append(clean_variable(df, var))

out = pd.concat(cleaned)
out_path = f"{OUTPUT_CLEAN}/cleaned_combined.csv"

out.to_csv(out_path, index=False)
print(f"✔ Cleaning complete → {out_path}")
