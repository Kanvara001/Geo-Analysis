import os
import pandas as pd
from functools import reduce

CLEAN_DIR = "gee-pipeline/outputs/clean"
OUTPUT_MERGED = "gee-pipeline/outputs/merged"
os.makedirs(OUTPUT_MERGED, exist_ok=True)

print("🔗 Merging cleaned parquet files...")

KEYS = ["province", "district", "subdistrict", "year", "month"]

parquet_files = []
for root, _, files in os.walk(CLEAN_DIR):
    for f in files:
        if f.endswith(".parquet"):
            parquet_files.append(os.path.join(root, f))

if not parquet_files:
    raise RuntimeError("❌ No cleaned parquet files found")

dfs = []

for f in parquet_files:
    df = pd.read_parquet(f)

    # normalize schema
    if "subdistric" in df.columns and "subdistrict" not in df.columns:
        df = df.rename(columns={"subdistric": "subdistrict"})

    missing = [k for k in KEYS if k not in df.columns]
    if missing:
        raise RuntimeError(f"❌ {f} missing merge keys: {missing}")

    dfs.append(df)

print(f"📦 Loaded {len(dfs)} cleaned parquet files")

df_merged = reduce(
    lambda l, r: pd.merge(l, r, on=KEYS, how="outer"),
    dfs
)

df_merged = df_merged.sort_values(KEYS)

output_path = os.path.join(OUTPUT_MERGED, "df_merge_new.parquet")
df_merged.to_parquet(output_path, index=False)

print(f"✅ Merged file saved: {output_path}")
