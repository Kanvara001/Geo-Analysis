import os
import time
import subprocess
import argparse

assert "scripts_auto" in __file__, "❌ Wrong script path"

RAW_DIR = "gee-pipeline/outputs/raw_parquet"

def main(var):
    bucket = os.environ.get("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("GCS_BUCKET not set")

    os.makedirs(RAW_DIR, exist_ok=True)

    print("⏳ Waiting for GEE tasks...")
    time.sleep(60)

    print("⬇️ Downloading parquet")
    subprocess.run(
        ["gsutil", "-m", "cp", "-r", f"gs://{bucket}/parquet/*", RAW_DIR],
        check=True
    )

    print("✅ Download done")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--var", required=True)
    args = parser.parse_args()

    main(args.var)
