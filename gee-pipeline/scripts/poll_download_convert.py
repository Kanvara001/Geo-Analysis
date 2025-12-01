import os
import json
import time
import pandas as pd
from google.cloud import storage
from google.api_core.exceptions import NotFound

BUCKET = os.getenv("GCS_BUCKET")

RAW_EXPORT_PREFIX = "raw_export/"
OUTPUT_DIR = "gee-pipeline/outputs/raw_parquet"
os.makedirs(OUTPUT_DIR, exist_ok=True)

client = storage.Client()


def safe_blob_exists(blob):
    """เช็คว่า blob มีอยู่จริงแบบไม่ให้ error"""
    try:
        blob.reload()
        return True
    except NotFound:
        return False


def download_and_convert(blob):
    """ดาวน์โหลด geojson → แปลงเป็น parquet"""
    local_geojson = os.path.join(OUTPUT_DIR, blob.name.split("/")[-1])

    print(f"⬇ Downloading: {blob.name}")
    blob.download_to_filename(local_geojson)

    df = pd.read_json(local_geojson)
    parquet_path = local_geojson.replace(".geojson", ".parquet")

    df.to_parquet(parquet_path)
    print(f"✔ Converted: {parquet_path}")

    os.remove(local_geojson)


def main():
    print("🔍 Checking bucket…")

    bucket = client.bucket(BUCKET)
    blobs = list(bucket.list_blobs(prefix=RAW_EXPORT_PREFIX))

    if not blobs:
        print("⚠ No exported files found in bucket.")
        return

    for blob in blobs:
        path = blob.name

        # ข้ามโฟลเดอร์
        if path.endswith("/"):
            continue

        # เช็คไฟล์ว่ามีจริงหรือไม่
        if not safe_blob_exists(blob):
            print(f"⚠ Skip missing file: {path}")
            continue

        # ข้ามไฟล์ที่ดาวน์โหลดไปแล้ว
        parquet_name = path.split("/")[-1].replace(".geojson", ".parquet")
        parquet_path = os.path.join(OUTPUT_DIR, parquet_name)

        if os.path.exists(parquet_path):
            print(f"✔ Already processed: {path}")
            continue

        # download + convert
        download_and_convert(blob)


if __name__ == "__main__":
    main()
