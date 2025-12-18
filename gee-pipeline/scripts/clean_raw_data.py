import os
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# Paths
# --------------------------------------------------
RAW_PARQUET_DIR = Path("gee-pipeline/outputs/raw_parquet")
CLEAN_DIR = Path("gee-pipeline/outputs/clean")

print("🔧 Cleaning raw data…")

# --------------------------------------------------
# Check raw parquet directory
# --------------------------------------------------
if not RAW_PARQUET_DIR.exists():
    raise FileNotFoundError(f"❌ RAW_PARQUET_DIR not found: {RAW_PARQUET_DIR}")

# --------------------------------------------------
# Find parquet files (recursive)
# --------------------------------------------------
parquet_files = list(RAW_PARQUET_DIR.rglob("*.parquet"))

if not parquet_files:
    raise RuntimeError("❌ No parquet files found in raw_parquet")

print(f"📦 Found {len(parquet_files)} parquet files")

# --------------------------------------------------
# Create clean output directory
# --------------------------------------------------
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# Clean each parquet file
# --------------------------------------------------
for pq_file in parquet_files:
    variable = pq_file.parent.name.upper()
    print(f"🧹 Cleaning {variable} → {pq_file.name}")

    df = pd.read_parquet(pq_file)

    # --------------------------------------------------
    # Rename value column → variable name
    # --------------------------------------------------
    key_cols = ["province", "district", "subdistrict", "year", "month"]
    value_cols = [c for c in df.columns if c not in key_cols]

    if len(value_cols) == 1:
        df = df.rename(columns={value_cols[0]: variable})
    else:
        print(f"⚠️  Warning: {pq_file.name} has multiple value columns: {value_cols}")

    # --------------------------------------------------
    # Save cleaned parquet (by variable)
    # --------------------------------------------------
    output_dir = CLEAN_DIR / variable
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / pq_file.name
    df.to_parquet(output_path, index=False)

print("✅ Cleaning completed successfully!")
