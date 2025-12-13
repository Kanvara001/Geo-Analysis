import os
import pandas as pd
from functools import reduce

# --------------------------------------------------
# Paths
# --------------------------------------------------
CLEAN_DIR = "gee-pipeline/outputs/clean"
OUTPUT_MERGED = "gee-pipeline/outputs/merged"
os.makedirs(OUTPUT_MERGED, exist_ok=True)

print("🔗 Merging cleaned parquet files...")

# --------------------------------------------------
# Load cleaned files
# --------------------------------------------------
files = [
    os.path.join(CLEAN_DIR, f)
    for f in os.listdir(CLEAN_DIR)
    if f.endswith(".parquet")
]

if not files:
    raise RuntimeError("❌ No cleaned parquet files found")

dfs = [pd.read_parquet(f) for f in files]

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
output_path = os.path.join(OUTPUT_MERGED, "df_merge_new.parquet")
df_merged.to_parquet(output_path, index=False)

print(f"✅ Merged file saved: {output_path}")
print("🎉 Merge completed successfully!")
