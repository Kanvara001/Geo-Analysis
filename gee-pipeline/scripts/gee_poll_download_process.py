import os
import time
import glob
import pandas as pd
import numpy as np
from utils_cleaning import clean_variables

# =====================================================================
# Settings
# =====================================================================
EXPORT_DIR = "exports_raw"
OUTPUT_DIR = "outputs"
WAIT_TIME = 3   # seconds between checks


# =====================================================================
# Helper: Wait for exported files to appear
# =====================================================================
def wait_for_exports(expected_count=5):
    """
    Wait until exported files appear in EXPORT_DIR.
    This is optional and depends on your workflow.
    """
    print("🔍 Checking for exported files...")
    waited = 0

    while True:
        files = glob.glob(f"{EXPORT_DIR}/*.csv")
        if len(files) >= expected_count:
            print(f"✅ Found {len(files)} exported CSV files")
            return files

        waited += WAIT_TIME
        print(f"⏳ Waiting... {waited}s (found {len(files)})")
        time.sleep(WAIT_TIME)


# =====================================================================
# Load & merge all CSVs exported by GEE
# =====================================================================
def load_exported_data():
    print("📥 Loading exported CSV files...")
    files = glob.glob(f"{EXPORT_DIR}/*.csv")

    if not files:
        raise RuntimeError("❌ No exported CSV files found in exports_raw/")

    df_list = []
    for f in files:
        print(f"  - Loading {os.path.basename(f)}")
        _df = pd.read_csv(f)

        # Must have date column
        if 'date' not in _df.columns:
            raise RuntimeError(f"❌ File {f} has no 'date' column")

        df_list.append(_df)

    print("🔗 Merging all tables...")
    merged = pd.concat(df_list, ignore_index=True)

    # Ensure date is datetime
    merged['date'] = pd.to_datetime(merged['date'])
    merged.sort_values(by='date', inplace=True)

    return merged


# =====================================================================
# Pivot table to wide format for cleaning
# =====================================================================
def reshape_to_wide(df):
    """
    Expect df to include:
    - date
    - tambon_id (or other area ID)
    - variable_name
    - value
    """

    if "variable" not in df.columns:
        raise RuntimeError("❌ Expected column 'variable' not found in merged CSVs")

    print("🧱 Reshaping long → wide...")

    wide = df.pivot_table(
        index=["date", "tambon_id"],
        columns="variable",
        values="value"
    ).reset_index()

    # rename columns cleanly
    wide.columns.name = None

    return wide


# =====================================================================
# Final save
# =====================================================================
def save_output(df):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "monthly_clean_dataframe.csv")
    df.to_csv(out_path, index=False)
    print(f"💾 Saved clean monthly dataframe → {out_path}")


# =====================================================================
# Main process
# =====================================================================
def main():
    print("🚀 Starting GEE download + cleaning pipeline")

    # 1) Optionally wait for exports (your workflow may skip this)
    files = glob.glob(f"{EXPORT_DIR}/*.csv")
    if not files:
        wait_for_exports()

    # 2) Load all CSVs
    df = load_exported_data()

    # 3) Reshape for cleaner
    df = reshape_to_wide(df)

    # 4) Clean variables using utils_cleaning.py
    df = clean_variables(df)

    # 5) Save final output
    save_output(df)

    print("🎉 Cleaning + merge pipeline finished!")


if __name__ == "__main__":
    main()
