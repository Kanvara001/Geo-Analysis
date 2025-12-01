import os
import time
from google.cloud import storage
import geopandas as gpd
from google.api_core.exceptions import NotFound

BUCKET = os.environ["GCS_BUCKET"]
RAW_PREFIX = "raw_export"
OUT_DIR = "gee-pipeline/outputs/raw_parquet"

os.makedirs(OUT_DIR, exist_ok=True)

storage_client = storage.Client.from_service_account_json(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
)

bucket = storage_client.bucket(BUCKET)

def list_geojson():
    blobs = bucket.list_blobs(prefix=RAW_PREFIX)
    return [b for b in blobs if ".geojson" in b.name]

def possible_names(blob_name):
    """Generate possible file names for broken GEE exports."""
    names = [blob_name]

    # Case: NDVI_2025_11.geojson.geojson -> NDVI_2025_11.geojson
    if blob_name.endswith(".geojson.geojson"):
        names.append(blob_name[:-8])  # remove last ".geojson"

    return names

def download_with_fallback(blob):
    """Try to download using multiple possible names."""
    tried = []

    for name in possible_names(blob.name):
        try:
            clean_local_name = os.path.basename(name)
            local_geojson = f"/tmp/{clean_local_name}"

            bucket.blob(name).download_to_filename(local_geojson)

            print(f"✔ Downloaded: {name}")
            return local_geojson

        except NotFound:
            tried.append(name)
            continue

    print("❌ All download attempts failed:", tried)
    raise NotFound("File not found in bucket.")

def convert_to_parquet(local_geojson):
    filename = os.path.basename(local_geojson).replace(".geojson", ".parquet")
    local_parquet = os.path.join(OUT_DIR, filename)

    gdf = gpd.read_file(local_geojson)
    gdf.to_parquet(local_parquet)

    print("✔ Converted:", local_parquet)

def main():
    print("🔍 Checking bucket…")

    files = list_geojson()
    if not files:
        print("⏳ No files found in bucket.")
        return

    for blob in files:
        local_gj = download_with_fallback(blob)
        convert_to_parquet(local_gj)

    print("🎉 All files processed successfully.")

if __name__ == "__main__":
    main()
