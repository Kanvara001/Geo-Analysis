import os
import pandas as pd
from glob import glob

assert "scripts_auto" in __file__, "❌ Wrong script path"

MERGED_PATH = "gee-pipeline/outputs/merged/merged_dataset.parquet"
CLEAN_DIR = "gee-pipeline/outputs/clean"

def main():
    clean_files = glob(f"{CLEAN_DIR}/*.parquet")
    if not clean_files:
        print("⚠️ No clean files")
        return

    new_df = pd.concat([pd.read_parquet(f) for f in clean_files])

    if os.path.exists(MERGED_PATH):
        old_df = pd.read_parquet(MERGED_PATH)
        df = pd.concat([old_df, new_df])
    else:
        df = new_df

    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset=["date", "province"]).sort_values("date")

    df.to_parquet(MERGED_PATH)
    print("✅ Merge complete")

if __name__ == "__main__":
    main()
