import os
import pandas as pd

PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CSV = "gee-pipeline/raw_export/combined.csv"

os.makedirs("gee-pipeline/raw_export", exist_ok=True)

def main():
    files = [f for f in os.listdir(PARQUET_DIR) if f.endswith(".parquet")]

    if not files:
        print("❌ No parquet files found.")
        return

    dfs = []
    for file in files:
        path = os.path.join(PARQUET_DIR, file)
        print("📥 Load:", path)
        dfs.append(pd.read_parquet(path))

    full = pd.concat(dfs, ignore_index=True)
    full.to_csv(OUTPUT_CSV, index=False)

    print("✅ Combined CSV saved at:", OUTPUT_CSV)

if __name__ == "__main__":
    main()
