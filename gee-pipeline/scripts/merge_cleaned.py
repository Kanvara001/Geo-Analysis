import os
import pandas as pd

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
CLEAN_DIR = "gee-pipeline/outputs/clean"
OUTPUT_MERGED = "/content/drive/MyDrive/geo_project/merged"

os.makedirs(OUTPUT_MERGED, exist_ok=True)

print("🔗 Merging cleaned parquet files...")

# ---------------------------------------------------------
# Find all cleaned parquet files
# ---------------------------------------------------------
files = [
    os.path.join(CLEAN_DIR, f)
    for f in os.listdir(CLEAN_DIR)
    if f.endswith(".parquet")
]

if not files:
    raise RuntimeError("❌ No cleaned parquet files found in gee-pipeline/outputs/clean")

# ---------------------------------------------------------
# Load + Merge
# ---------------------------------------------------------
dfs = [pd.read_parquet(f) for f in files]
df_merged = pd.concat(dfs, ignore_index=True)

# ---------------------------------------------------------
# Sort nicely
# ---------------------------------------------------------
sort_cols = [
    "province", "amphoe", "tambon",
    "variable", "year", "month"
]

for col in sort_cols:
    if col in df_merged.columns:
        df_merged = df_merged.sort_values(sort_cols)

# ---------------------------------------------------------
# Save final merged parquet
# ---------------------------------------------------------
output_path = os.path.join(OUTPUT_MERGED, "df_merge_new.parquet")
df_merged.to_parquet(output_path, index=False)

print(f"✅ Merged file saved to: {output_path}")
print("🎉 Merge completed successfully.")
