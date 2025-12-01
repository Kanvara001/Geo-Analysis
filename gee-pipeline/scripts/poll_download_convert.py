import os
import time
from google.cloud import storage
import geopandas as gpd
import pandas as pd

BUCKET = os.environ["GCS_BUCKET"]
RAW_PREFIX = "raw_export"
OUT_DIR = "gee-pipeline/outputs/raw_parquet"

os.makedirs(OUT_DIR, exist_ok=True)

# Authenticate with service account
client = storage.Client.from_service_account_json(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
)
bucket = client.bucket(BUCKET)


def list_geojson():
    blobs = bucket.list_blobs(prefix=RAW_PREFIX)
    return [b for b in blobs if b.name.endswith(".geojson")]


def download_and_convert(blob):
    print(f"⬇ Downloaded: {blob.name}")

    local_geo = f"/tmp/{os.path.basename(blob.name)}"
    local_parquet = os.path.join(
        OUT_DIR,
        os.path.basename(blob.name).replace(".geojson", ".parquet")
    )

    # Download file
    blob.download_to_filename(local_geo)

    # Read geojson safely
    try:
        gdf = gpd.read_file(local_geo)
    except Exception:
        print("⚠ GeoPandas failed — fallback to Pandas")
        df = pd.read_json(local_geo)
        df.to_parquet(local_parquet)
        print("✔ Converted →", local_parquet)
        return

    # Drop geometry to avoid PyArrow error
    if "geometry" in gdf.columns:
        gdf = gdf.drop(columns=["geometry"])

    gdf.to_parquet(local_parquet)
    print("✔ Converted →", local_parquet)


def main():
    print("🔍 Checking bucket…")

    waited = 0
    while waited < 900:  # Wait up to 15 minutes
        files = list_geojson()

        if len(files) == 0:
            print("⏳ No exported files yet — waiting 30s…")
            time.sleep(30)
            waited += 30
            continue

        # Process files
        for blob in files:
            download_and_convert(blob)

        print("🎉 All GeoJSON converted.")
        return

    print("❌ Timeout: No files received from GEE")
    exit(1)


if __name__ == "__main__":
    main()
