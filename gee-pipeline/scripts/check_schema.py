import os
import pandas as pd
from collections import defaultdict

RAW_DIR = "gee-pipeline/outputs/raw_parquet"
CLEAN_DIR = "gee-pipeline/outputs/clean"

EXPECTED_KEYS = {"province", "district", "subdistrict", "year", "month"}

def check_folder(folder):
    print(f"\n📂 Checking folder: {folder}")
    schema_map = defaultdict(list)

    for root, _, files in os.walk(folder):
        for f in files:
            if f.endswith(".parquet"):
                path = os.path.join(root, f)
                try:
                    df = pd.read_parquet(path, engine="pyarrow")
                    cols = set(df.columns)
                    missing = EXPECTED_KEYS - cols
                    extra = cols - EXPECTED_KEYS

                    schema_map[str(cols)].append(path)

                    if missing:
                        print(f"❌ {path}")
                        print(f"   Missing columns: {missing}")
                    else:
                        print(f"✅ {path}")

                except Exception as e:
                    print(f"🔥 Failed to read {path}: {e}")

    print("\n📊 Schema summary:")
    for schema, files in schema_map.items():
        print(f"\nSchema ({len(files)} files):")
        print(schema)

# -------------------------------
# Run checks
# -------------------------------
if os.path.exists(RAW_DIR):
    check_folder(RAW_DIR)
else:
    print("⚠️ raw_parquet not found")

if os.path.exists(CLEAN_DIR):
    check_folder(CLEAN_DIR)
else:
    print("⚠️ clean folder not found")
