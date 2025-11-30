import os
import time
from google.cloud import storage
import pandas as pd
import pyarrow.parquet as pq

BUCKET = os.environ["GCS_BUCKET"]
RAW_DIR = "raw_export"

client = storage.Client()

def list_new_files():
    bucket = client.bucket(BUCKET)
    return [
        blob.name for blob in bucket.list_blobs()
        if blob.name.startswith(RAW_DIR) and blob.name.endswith(".geojson")
    ]

def download_and_convert(blobs):
    os.makedirs("gee-pipeline/outputs/raw_parquet", exist_ok=True)

    for b in blobs:
        filename = b.split("/")[-1]
        local_geo = f"gee-pipeline/outputs/raw_parquet/{filename}"

        bucket = client.bucket(BUCKET)
        blob = bucket.blob(b)
        blob.download_to_filename(local_geo)

        # Convert to parquet
        df = pd.read_json(local_geo)
        parquet_path = local_geo.replace(".geojson", ".parquet")
        df.to_parquet(parquet_path)

        print(f"Converted → {parquet_path}")

def main():
    while True:
        files = list_new_files()
        if files:
            download_and_convert(files)
            break

        print("Waiting for GEE tasks...")
        time.sleep(15)

if __name__ == "__main__":
    main()
