# === clean_raw_data.py (FINAL) ===

import pandas as pd
import glob
import os

RAW_DIR = "gee-pipeline/outputs/raw_parquet"
OUT_DIR = "gee-pipeline/outputs/clean"
os.makedirs(OUT_DIR, exist_ok=True)

def load_all():
    files = glob.glob(f"{RAW_DIR}/*.parquet")
    if not files:
        print("⚠ No raw parquet found.")
        return None
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

df = load_all()
if df is None:
    exit(0)

needed_cols = ["province", "amphoe", "tambon", "year", "month", "variable"]

missing = [c for c in needed_cols if c not in df.columns]
if missing:
    print("❌ Missing columns in raw data:", missing)
    print("⚠ Your GEE export is missing properties. Fix gee_export_tasks.py")
    exit(1)

df["value"] = (
    pd.to_numeric(df.get("mean"), errors="coerce")
    .fillna(pd.to_numeric(df.get("count"), errors="coerce"))
)

df = df.dropna(subset=["value"])

out_list = []

for var in df["variable"].unique():
    t = df[df["variable"] == var].copy()
    t = t.sort_values(["province", "amphoe", "tambon", "year", "month"])

    t["clean_value"] = t["value"].groupby(
        [t["province"], t["amphoe"], t["tambon"]]
    ).transform(lambda s: s.interpolate().bfill().ffill())

    out_list.append(t)

out = pd.concat(out_list)
out.to_csv(f"{OUT_DIR}/cleaned_combined.csv", index=False)

print("✔ Clean complete → cleaned_combined.csv")
