import pandas as pd
from pathlib import Path
from functools import reduce

# --------------------------------------------------
# Paths
# --------------------------------------------------
CLEAN_DIR = Path("gee-pipeline/outputs/clean")
OUTPUT_DIR = Path("gee-pipeline/outputs/merged")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["province", "district", "subdistrict", "year", "month"]

print("🔗 Merging cleaned parquet files...")

# --------------------------------------------------
# Load & concat per variable
# --------------------------------------------------
dfs_per_var = []

for var_dir in CLEAN_DIR.iterdir():
    if not var_dir.is_dir():
        continue

    parquet_files = list(var_dir.glob("*.parquet"))
    if not parquet_files:
        continue

    print(f"📦 Processing {var_dir.name} ({len(parquet_files)} files)")

    df_var = pd.concat(
        [pd.read_parquet(f) for f in parquet_files],
        ignore_index=True
    )

    dfs_per_var.append(df_var)

if not dfs_per_var:
    raise RuntimeError("❌ No cleaned parquet data found")

# --------------------------------------------------
# Merge all variables (wide format)
# --------------------------------------------------
df_merged = reduce(
    lambda left, right: pd.merge(left, right, on=KEYS, how="outer"),
    dfs_per_var
)

# --------------------------------------------------
# Sort
# --------------------------------------------------
df_merged = df_merged.sort_values(KEYS)

# --------------------------------------------------
# Save
# --------------------------------------------------
output_path = OUTPUT_DIR / "df_merge_new.parquet"
df_merged.to_parquet(output_path, index=False)

print(f"✅ Merged parquet saved: {output_path}")
print("🎉 Merge completed successfully!")
