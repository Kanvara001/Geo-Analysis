import os
import pandas as pd
from functools import reduce

# ----------------------------------------
# CONFIG
# ----------------------------------------
FILL_DIR = "gee-pipeline/outputs/fill"      # 🔥 เปลี่ยนจาก clean → fill
OUTPUT_DIR = "gee-pipeline/outputs/merged"
os.makedirs(OUTPUT_DIR, exist_ok=True)

KEYS = ["province", "district", "subdistrict", "year", "month"]

print("🔗 Merging FILLED parquet files...")

variable_dfs = {}

# --------------------------------------------------
# 1) Load & concat per variable
# --------------------------------------------------
for variable in os.listdir(FILL_DIR):
    var_dir = os.path.join(FILL_DIR, variable)
    if not os.path.isdir(var_dir):
        continue

    files = [
        os.path.join(var_dir, f)
        for f in os.listdir(var_dir)
        if f.endswith(".parquet")
    ]

    if len(files) == 0:
        print(f"⚠️ Skip {variable} — no parquet files")
        continue

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
if len(variable_dfs) == 0:
    raise RuntimeError("❌ No filled variables found to merge!")

df_merged = reduce(
    lambda l, r: pd.merge(l, r, on=KEYS, how="outer"),
    variable_dfs.values()
)

df_merged = df_merged.sort_values(KEYS)

output_path = os.path.join(OUTPUT_DIR, "merged_dataset.parquet")
df_merged.to_parquet(output_path, index=False)

print(f"✅ Merge completed: {output_path}")
