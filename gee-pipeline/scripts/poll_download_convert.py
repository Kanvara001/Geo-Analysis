import os
import time
import json
import pandas as pd
from google.cloud import storage

BUCKET = "geo-analysis-472713-bucket"
RAW_PREFIX = "raw_export"
LOCAL_RAW = "gee-pipeline/outputs/raw_geojson"
LOCAL_PARQUET = "gee-pipeline/outputs/raw_parquet"

os.makedirs(LOCAL_RAW, exist_ok=True)
os.makedirs(LOCAL_PARQUET, exist_ok=True)

storage_client = storage.Client.from_service_account_json(
    "gee-pipeline/service-key.json"
)
bucket = storage_client.bucket(BUCKET)


def download_and_convert(blob):
    basename = blob.name.split("/")[-1]   # NDVI_2025_11.geojson
    local_geojson = f"{LOCAL_RAW}/{basename}"
    local_parquet = f"{LOCAL_PARQUET}/{basename.replace('.geojson', '.parquet')}"

    print(f"⬇ Downloaded: {blob.name}")
    blob.download_to_filename(local_geojson)

    df = pd.read_json(local_geojson)
    df.to_parquet(local_parquet, index=False)
    print(f"✔ Converted → {local_parquet}")


def main():
    print("🔍 Checking bucket…")

    blobs = bucket.list_blobs(prefix=RAW_PREFIX)

    for blob in blobs:
        if blob.name.endswith(".geojson"):
            download_and_convert(blob)

    print("🏁 Done — all downloaded & converted.")


if __name__ == "__main__":
    main()
