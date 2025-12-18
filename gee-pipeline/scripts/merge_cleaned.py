import os
import pandas as pd
from pathlib import Path
from functools import reduce

# --------------------------------------------------
# Paths
# --------------------------------------------------
CLEAN_DIR = Path("gee-pipeline/outputs/clean")
OUTPUT_MERGED = Path("gee-pipeline/outputs/merged")
OUTPUT_MERGED.mkdir(parents=True, exist_ok=True)

print("🔗 Merging cleaned parquet files...")

# --------------------------------------------------
# Find all cleaned parquet files (recursive)
# --------------------------------------------------
parquet_files = list(CLEAN_DIR.rglob("*.parquet"))

if not parquet_files:
    raise RuntimeError("❌ No cleaned parquet files found")

print(f"📦 Found {len(parquet_files)} cleaned parquet files")

# --------------------------------------------------
# Load dataframes
# --------------------------------------------------
dfs = []
for f in parquet_files:
    print(f"📥 Loading {f}")
    dfs.append(pd.read_parquet(f))

# --------------------------------------------------
# Merge (wide format)
# --------------------------------------------------
KEYS = ["province", "district", "subdistrict", "year", "month"]

df_merged = reduce(
    lambda left, right: pd.merge(left, right, on=KEYS, how="outer"),
    dfs
)

# --------------------------------------------------
# Sort
# --------------------------------------------------
df_merged = df_merged.sort_values(KEYS)

# --------------------------------------------------
# Save merged parquet
# --------------------------------------------------
output_path = OUTPUT_MERGED / "df_merge_new.parquet"
df_merged.to_parquet(output_path, index=False)

print(f"✅ Merged file saved: {output_path}")
print("🎉 Merge completed successfully!")
