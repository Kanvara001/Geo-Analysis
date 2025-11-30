import os
import json
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

def list_all_geojson():
    blobs = bucket.list_blobs(prefix=RAW_PREFIX)
    return [b for b in blobs if b.name.endswith(".geojson")]

def download_and_convert(blob):
    # ---- FIX #1: ให้ basename ถูกต้องเสมอ ----
    file_name = os.path.basename(blob.name)

    # ---- FIX #2: ไม่มีการซ้อน .geojson.geojson ----
    if file_name.endswith(".geojson"):
        base = file_name[:-8]   # ตัด ".geojson"
    else:
        raise ValueError(f"Unexpected file extension: {file_name}")

    local_geojson = f"/tmp/{file_name}"
    local_parquet = os.path.join(
        OUT_DIR,
        f"{base}.parquet"
    )

    print(f"⬇ Downloading {blob.name}")
    blob.download_to_filename(local_geojson)

    gdf = gpd.read_file(local_geojson)
    gdf.to_parquet(local_parquet)

    print("✔ Converted:", local_parquet)

def main():
    print("🔍 Checking bucket for completed exports…")

    while True:
        files = list_all_geojson()

        if len(files) == 0:
            print("⏳ No exported files yet. Waiting 30 sec…")
            time.sleep(30)
            continue

        for blob in files:
            download_and_convert(blob)

        print("🎉 All files downloaded and converted.")
        break

if __name__ == "__main__":
    main()
