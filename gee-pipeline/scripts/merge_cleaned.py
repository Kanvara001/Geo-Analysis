import os
import pandas as pd
from functools import reduce

CLEAN_DIR = "gee-pipeline/outputs/clean"
OUTPUT_DIR = "gee-pipeline/outputs/merged"
os.makedirs(OUTPUT_DIR, exist_ok=True)

KEYS = ["province", "district", "subdistrict", "year", "month"]

print("🔗 Merging cleaned parquet files (correct way)...")

variable_dfs = {}

# --------------------------------------------------
# 1) Load & concat per variable
# --------------------------------------------------
for variable in os.listdir(CLEAN_DIR):
    var_dir = os.path.join(CLEAN_DIR, variable)
    if not os.path.isdir(var_dir):
        continue

    files = [
        os.path.join(var_dir, f)
        for f in os.listdir(var_dir)
        if f.endswith(".parquet")
    ]

    dfs = [pd.read_parquet(f) for f in files]
    df_var = pd.concat(dfs, ignore_index=True)

    # keep only merge keys + variable
    keep_cols = KEYS + [variable]
    df_var = df_var[keep_cols]

    variable_dfs[variable] = df_var
    print(f"📦 {variable}: {len(df_var)} rows")

# --------------------------------------------------
# 2) Merge across variables
# --------------------------------------------------
df_merged = reduce(
    lambda l, r: pd.merge(l, r, on=KEYS, how="outer"),
    variable_dfs.values()
)

df_merged = df_merged.sort_values(KEYS)

output_path = os.path.join(OUTPUT_DIR, "merged_dataset.parquet")
df_merged.to_parquet(output_path, index=False)

print(f"✅ Merge completed: {output_path}")
