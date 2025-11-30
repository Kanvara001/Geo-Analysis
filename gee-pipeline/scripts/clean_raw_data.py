import pandas as pd
import numpy as np
import glob
import os

RAW_DIR = "gee-pipeline/outputs/raw_parquet"
OUT_DIR = "gee-pipeline/outputs/clean"
os.makedirs(OUT_DIR, exist_ok=True)

def load_all():
    files = glob.glob(f"{RAW_DIR}/*.parquet")
    if len(files) == 0:
        print("⚠ No parquet files found. Skipping clean step.")
        return None
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

df = load_all()
if df is None:
    exit(0)

df["value"] = pd.to_numeric(df["mean"], errors="ignore").fillna(
    pd.to_numeric(df["count"], errors="ignore")
)

df = df.dropna(subset=["value"])

LONG_GAP = {"NDVI": 2, "LST": 2, "SoilMoisture": 1}

out_list = []
for var, th in LONG_GAP.items():

    tmp = df[df["variable"] == var].copy()
    tmp = tmp.sort_values(["province","amphoe","tambon","year","month"])

    def filler(g):
        s = g["value"]
        if s.isna().sum() >= th:
            return s.fillna(s.mean())
        return s.interpolate().fillna(method="bfill").fillna(method="ffill")

    tmp["clean_value"] = tmp.groupby(["province","amphoe","tambon"])["value"].transform(filler)
    out_list.append(tmp)

out = pd.concat(out_list)
out.to_csv(f"{OUT_DIR}/cleaned_combined.csv", index=False)
print("✔ Clean complete → cleaned_combined.csv")
