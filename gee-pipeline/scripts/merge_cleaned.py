import os
import pandas as pd

# ---------------------------------------------------------
# PATH SETTINGS
# ---------------------------------------------------------
CLEAN_DIR = "gee-pipeline/outputs/clean"
OUTPUT_DIR = "/content/drive/MyDrive/geo_project/merged"
OUTPUT_FILE = "df_merge_new.parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🔄 Starting merge of cleaned parquet files...\n")

# ---------------------------------------------------------
# CHECK CLEAN DIRECTORY
# ---------------------------------------------------------
if not os.path.exists(CLEAN_DIR):
    raise FileNotFoundError(f"❌ CLEAN_DIR does not exist: {CLEAN_DIR}")

files = [f for f in os.listdir(CLEAN_DIR) if f.endswith(".parquet")]

if len(files) == 0:
    raise RuntimeError("❌ No cleaned parquet files found in folder: gee-pipeline/outputs/clean")

print(f"📦 Found {len(files)} cleaned parquet files to merge.\n")

# ---------------------------------------------------------
# LOAD ALL CLEAN FILES
# ---------------------------------------------------------
dfs = []

for f in files:
    path = os.path.join(CLEAN_DIR, f)
    print(f"📥 Loading {path}")

    try:
        df = pd.read_parquet(path)
        dfs.append(df)
    except Exception as e:
        print(f"⚠️ Warning: Failed to load {path} — {e}")

if len(dfs) == 0:
    raise RuntimeError("❌ No valid parquet files loaded. Cannot merge.")

# ---------------------------------------------------------
# CONCAT ALL CLEANED DATA
# ---------------------------------------------------------
print("\n🔗 Concatenating all dataframes...")
merged = pd.concat(dfs, ignore_index=True)

# ---------------------------------------------------------
# VALIDATE REQUIRED COLUMNS
# ---------------------------------------------------------
required_cols = ["province", "amphoe", "tambon", "variable", "date", "clean_value"]

missing = [c for c in required_cols if c not in merged.columns]
if missing:
    raise RuntimeError(f"❌ Missing required columns in merged dataframe: {missing}")

# ---------------------------------------------------------
# SORT FOR ANALYSIS CONSISTENCY
# ---------------------------------------------------------
print("📊 Sorting merged dataframe...")
merged = merged.sort_values(["province", "amphoe", "tambon", "variable", "date"])


# ---------------------------------------------------------
# SAVE MERGED PARQUET
# ---------------------------------------------------------
output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
merged.to_parquet(output_path, index=False)

print("\n✅ SUCCESS! Merged file saved at:")
print(f"   {output_path}")
print(f"\n🎉 Merge completed with {len(merged):,} rows.")
