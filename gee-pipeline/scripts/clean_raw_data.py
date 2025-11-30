import pandas as pd
import numpy as np
import os
import glob
import json

RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CLEAN = "gee-pipeline/outputs/clean"

os.makedirs(OUTPUT_CLEAN, exist_ok=True)

# -------------------------
# Load all parquet files
# -------------------------
def load_all():
    files = glob.glob(f"{RAW_PARQUET_DIR}/*.parquet")

    if len(files) == 0:
        raise FileNotFoundError("❌ No parquet files found in raw_parquet/. Run poll_download_convert.py first.")

    dfs = []
    for f in files:
        df = pd.read_parquet(f)

        # Flatten features if exists
        if "features" in df.columns:
            df = df.explode("features").reset_index(drop=True)
            df["properties"] = df["features"].apply(lambda x: x.get("properties"))
            props = pd.json_normalize(df["properties"])
            df = pd.concat([df.drop(columns=["features", "properties"]), props], axis=1)

        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


df = load_all()

# -------------------------
# Cleaning interpolation rules
# -------------------------
LONG_GAP_THRESHOLD = {
    "NDVI": 2,
    "LST": 2,
    "SoilMoisture": 1,
}

# -------------------------
# Clean per variable
# -------------------------
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()
    temp["value"] = pd.to_numeric(temp["value"], errors="coerce")

    # Sort
    temp = temp.sort_values(["province", "amphoe", "tambon", "year", "month"])

    def fill_series(g):
        s = g["value"]

        # Count longest gap
        long_gap = s.isna().astype(int).groupby((s.notna()).cumsum()).transform("count").max()

        if long_gap >= LONG_GAP_THRESHOLD[var]:
            climatology = s.groupby(g["month"]).transform("mean")
            new = s.fillna(climatology)
        else:
            new = s.interpolate()

        return new

    df_clean = temp.groupby(["province", "amphoe", "tambon"]).apply(fill_series)
    temp["clean_value"] = df_clean.values
    return temp


# -------------------------
# Run cleaning
# -------------------------
cleaned = []
for var in df["variable"].unique():
    cleaned.append(clean_variable(df, var))

out = pd.concat(cleaned)
out.to_csv(f"{OUTPUT_CLEAN}/cleaned_combined.csv", index=False)

print("🎉 Cleaning complete → cleaned_combined.csv")
