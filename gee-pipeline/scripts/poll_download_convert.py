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

def list_geojson_files():
    """Accept .geojson and .geojson.geojson"""
    blobs = bucket.list_blobs(prefix=RAW_PREFIX)
    out = []
    for b in blobs:
        if b.name.endswith(".geojson") or b.name.endswith(".geojson.geojson"):
            out.append(b)
    return out

def normalize_filename(name: str):
    """Remove duplicated `.geojson.geojson` → `.geojson`"""
    if name.endswith(".geojson.geojson"):
        return name[:-8]  # remove the second ".geojson"
    return name

def download_and_convert(blob):
    clean_name = normalize_filename(blob.name)

    local_geojson = f"/tmp/{os.path.basename(clean_name)}"
    local_parquet = os.path.join(
        OUT_DIR,
        os.path.basename(clean_name).replace(".geojson", ".parquet")
    )

    blob.download_to_filename(local_geojson)

    gdf = gpd.read_file(local_geojson)
    gdf.to_parquet(local_parquet)

    print("✔ Converted:", local_parquet)

def main():
    print("🔍 Checking bucket…")

    while True:
        files = list_geojson_files()

        if len(files) == 0:
            print("⏳ No files found yet. Waiting 30 sec…")
            time.sleep(30)
            continue

        for blob in files:
            download_and_convert(blob)

        print("🎉 All files processed.")
        break

if __name__ == "__main__":
    main()
