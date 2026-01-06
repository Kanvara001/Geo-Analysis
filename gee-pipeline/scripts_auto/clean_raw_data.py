import os
import pandas as pd
from glob import glob

assert "scripts_auto" in __file__, "❌ Wrong script path"

RAW_DIR = "gee-pipeline/outputs/raw_parquet"
CLEAN_DIR = "gee-pipeline/outputs/clean"

os.makedirs(CLEAN_DIR, exist_ok=True)

def main():
    files = glob(f"{RAW_DIR}/*.parquet")

    for f in files:
        name = os.path.basename(f)
        out = os.path.join(CLEAN_DIR, name)

        if os.path.exists(out):
            continue

        df = pd.read_parquet(f)
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna()

        df.to_parquet(out)
        print(f"🧹 Cleaned {name}")

    print("✅ Clean complete")

if __name__ == "__main__":
    main()
