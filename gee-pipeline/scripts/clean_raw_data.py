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

REQUIRED = ["province","amphoe","tambon","variable","year","month"]
missing = [c for c in REQUIRED if c not in df.columns]
if missing:
    print("❌ Missing columns:", missing)
    exit(1)

df["value"] = (
    pd.to_numeric(df.get("mean"), errors="coerce")
    .fillna(pd.to_numeric(df.get("count"), errors="coerce"))
)

df = df.dropna(subset=["value"])

out_list = []
for var in df["variable"].unique():

    tmp = df[df["variable"] == var].copy()
    tmp = tmp.sort_values(["province","amphoe","tambon","year","month"])

    def fill_series(s):
        if s.isna().sum() > 1:
            return s.fillna(s.mean())
        return s.interpolate().bfill().ffill()

    tmp["clean_value"] = tmp.groupby(["province","amphoe","tambon"])["value"].transform(fill_series)
    out_list.append(tmp)

out = pd.concat(out_list)
out.to_csv(f"{OUT_DIR}/cleaned_combined.csv", index=False)

print("✔ Clean complete → cleaned_combined.csv")
