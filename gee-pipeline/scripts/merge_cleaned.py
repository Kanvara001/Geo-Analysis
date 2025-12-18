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
# Merge keys (ยึด schema จริงจาก GEE)
# --------------------------------------------------
KEYS = ["province", "district", "subdistric", "year", "month"]

# --------------------------------------------------
# Load cleaned parquet files (recursive)
# --------------------------------------------------
parquet_files = []
for root, _, files in os.walk(CLEAN_DIR):
    for f in files:
        if f.endswith(".parquet"):
            parquet_files.append(os.path.join(root, f))

if not parquet_files:
    raise RuntimeError("❌ No cleaned parquet files found")

dfs = []

for f in parquet_files:
    print(f"📥 Loading {f}")
    df = pd.read_parquet(f)

    # ตรวจ schema ขั้นต่ำ
    missing = [k for k in KEYS if k not in df.columns]
    if missing:
        raise RuntimeError(f"❌ {f} missing columns: {missing}")

    dfs.append(df)

print(f"📦 Loaded {len(dfs)} cleaned parquet files")

# --------------------------------------------------
# Merge (wide format)
# --------------------------------------------------
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
