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
}

def load_all():
    files = glob.glob(f"{RAW_PARQUET_DIR}/*.parquet")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

df = load_all()

# Handle missing
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()

    temp["value"] = pd.to_numeric(temp["value"], errors="coerce")

    temp = temp.sort_values(["province", "amphoe", "tambon", "year", "month"])

    def fill_series(g):
        s = g["value"]

        long_gap = s.isna().astype(int).groupby(
            (s.notna()).cumsum()
        ).transform("count").max()

        if long_gap >= LONG_GAP_THRESHOLD[var]:
            # Fill by monthly climatology
            climatology = s.groupby(g["month"]).transform("mean")
            new = s.fillna(climatology)
        else:
            new = s.interpolate()

        return new

    df_clean = temp.groupby(["province", "amphoe", "tambon"]).apply(fill_series)
    temp["clean_value"] = df_clean.values
    return temp

cleaned = []
for var in df["variable"].unique():
    cleaned.append(clean_variable(df, var))

out = pd.concat(cleaned)
out.to_csv(f"{OUTPUT_CLEAN}/cleaned_combined.csv", index=False)

print("Cleaning complete → cleaned_combined.csv")
