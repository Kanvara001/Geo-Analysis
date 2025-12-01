import os
import time
from google.cloud import storage
import geopandas as gpd

BUCKET = os.environ["GCS_BUCKET"]
RAW_PREFIX = "raw_export"
ARCHIVE_PREFIX = "archive"
OUT_DIR = "gee-pipeline/outputs/raw_parquet"

os.makedirs(OUT_DIR, exist_ok=True)

storage_client = storage.Client.from_service_account_json(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
)

bucket = storage_client.bucket(BUCKET)

def list_all_geojson():
    return [b for b in bucket.list_blobs(prefix=RAW_PREFIX) if b.name.endswith(".geojson")]

def download_and_convert(blob):
    filename = os.path.basename(blob.name)
    
    # Detect metadata from filename
    variable, year, month = filename.replace(".geojson", "").split("_")
    year = int(year)
    month = int(month)

    local_geojson = f"/tmp/{filename}"
    local_parquet = f"/tmp/{variable}.parquet"

    # Download raw GeoJSON
    blob.download_to_filename(local_geojson)

    # Convert
    gdf = gpd.read_file(local_geojson)
    gdf.to_parquet(local_parquet)

    # Upload to GCS archive/YYYY/MM/variable.parquet
    archive_path = f"{ARCHIVE_PREFIX}/{year}/{month:02d}/{variable}.parquet"
    bucket.blob(archive_path).upload_from_filename(local_parquet)

    print(f"✔ Uploaded to archive: {archive_path}")

def main():
    print("🔍 Checking bucket…")

    while True:
        files = list_all_geojson()

        if len(files) == 0:
            print("⏳ No files yet. Waiting 30 sec…")
            time.sleep(30)
            continue

        for blob in files:
            download_and_convert(blob)

        print("🎉 Completed.")
        break

if __name__ == "__main__":
    main()
