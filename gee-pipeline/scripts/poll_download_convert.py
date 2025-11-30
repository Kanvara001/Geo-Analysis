# === poll_download_convert.py (FINAL) ===

import os
import time
from google.cloud import storage
import geopandas as gpd

BUCKET = os.environ["GCS_BUCKET"]
RAW_PREFIX = "raw_export"
OUT_DIR = "gee-pipeline/outputs/raw_parquet"

os.makedirs(OUT_DIR, exist_ok=True)

storage_client = storage.Client.from_service_account_json(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
)
bucket = storage_client.bucket(BUCKET)

def list_geojson():
    return [b for b in bucket.list_blobs(prefix=RAW_PREFIX) if b.name.endswith(".geojson")]

def download_and_convert(blob):
    local_geojson = f"/tmp/{os.path.basename(blob.name)}"
    local_parquet = os.path.join(
        OUT_DIR, os.path.basename(blob.name).replace(".geojson", ".parquet")
    )

    if os.path.exists(local_parquet):
        print("⏭ Skip (already exists):", local_parquet)
        return

    try:
        blob.download_to_filename(local_geojson)
    except Exception as e:
        print("❌ Download failed:", blob.name, e)
        return

    gdf = gpd.read_file(local_geojson)
    gdf.to_parquet(local_parquet)
    print("✔ Converted:", local_parquet)

def main():
    print("🔍 Checking bucket for completed exports…")

    while True:
        files = list_geojson()

        if len(files) == 0:
            print("⏳ No files yet. Wait 30 sec…")
            time.sleep(30)
            continue

        for blob in files:
            download_and_convert(blob)

        print("🎉 All files downloaded & converted.")
        break

if __name__ == "__main__":
    main()
