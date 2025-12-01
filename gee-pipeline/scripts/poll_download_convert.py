import os
import json
from google.cloud import storage
import pandas as pd

BUCKET = os.environ["GCS_BUCKET"]
client = storage.Client.from_service_account_json("gee-pipeline/service-key.json")

RAW_DIR = "gee-pipeline/outputs/raw_geojson"
PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

def list_all_geojson_blobs():
    """List ALL geojson files even if nested by variable/year/month."""
    bucket = client.bucket(BUCKET)
    return bucket.list_blobs(prefix="raw_export/")

def download_and_convert(blob):
    blob_path = blob.name
    if not blob_path.endswith(".geojson"):
        return

    print("⬇ Download:", blob_path)

    local_path = os.path.join(RAW_DIR, blob_path.replace("/", "_"))
    parquet_path = os.path.join(PARQUET_DIR, blob_path.replace("/", "_").replace(".geojson", ".parquet"))

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    blob.download_to_filename(local_path)

    df = pd.read_json(local_path)
    df.to_parquet(parquet_path)
    print("✔ Saved parquet:", parquet_path)

def main():
    print("🔍 Checking bucket…")
    blobs = list_all_geojson_blobs()

    for blob in blobs:
        download_and_convert(blob)

if __name__ == "__main__":
    main()
