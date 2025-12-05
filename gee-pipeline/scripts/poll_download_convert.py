import os
import json
import time
import pandas as pd
from google.cloud import storage

BUCKET_NAME = os.environ["GCS_BUCKET"]
RAW_DIR = "raw_export"

# Save Parquet here (match clean_raw_data.py)
RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
os.makedirs(RAW_PARQUET_DIR, exist_ok=True)

client = storage.Client()

def list_files():
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=RAW_DIR)
    return [b for b in blobs if b.name.endswith(".geojson")]

def safe_flatten_feature(f):
    props = f.get("properties", {})
    props.pop("geometry", None)
    f.pop("geometry", None)

    clean = {}
    for k, v in props.items():
        if isinstance(v, (list, dict)):
            continue
        clean[k] = v
    return clean

def download_and_convert(files):
    bucket = client.bucket(BUCKET_NAME)

    for blob in files:
        print(f"⬇ Downloading {blob.name} ...")
        content = blob.download_as_text()
        data = json.loads(content)
        rows = [safe_flatten_feature(f) for f in data.get("features", [])]

        df = pd.DataFrame(rows)

        # Ensure correct final filenames
        filename = blob.name.split("/")[-1].replace(".geojson", "")
        local_parquet = f"{RAW_PARQUET_DIR}/{filename}.parquet"
        local_json = f"{RAW_PARQUET_DIR}/{filename}.json"

        # Save JSON (optional)
        with open(local_json, "w") as f:
            json.dump(rows, f)

        # Save PARQUET
        print(f"➡ Converting to {local_parquet}")
        df.to_parquet(local_parquet, index=False)

def main():
    print("📡 Checking Google Cloud Storage...")
    files = list_files()

    if not files:
        print("⚠ No files found in GCS. Maybe export still running.")
        return

    print(f"Found {len(files)} files.")
    download_and_convert(files)
    print("🎉 Conversion complete!")

if __name__ == "__main__":
    main()
