import os
import rasterio
import pandas as pd
from google.cloud import storage
import numpy as np

BUCKET = os.environ["GCS_BUCKET"]
LOCAL_TMP = "gee-pipeline/tmp_tif"
OUT_DIR = "gee-pipeline/outputs/raw_parquet"

os.makedirs(LOCAL_TMP, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)


# ----------------------------------------------------------------------
# Convert TIFF → Parquet
# ----------------------------------------------------------------------
def tif_to_parquet(tif_path, out_path):
    print(f"🔄 Converting TIFF → Parquet: {tif_path}")

    with rasterio.open(tif_path) as src:
        arr = src.read(1)
        transform = src.transform

    rows, cols = arr.shape
    xs, ys = np.meshgrid(np.arange(cols), np.arange(rows))

    lon, lat = rasterio.transform.xy(transform, ys, xs)
    lon = np.array(lon).flatten()
    lat = np.array(lat).flatten()
    vals = arr.flatten()

    mask = ~np.isnan(vals)
    df = pd.DataFrame({
        "lon": lon[mask],
        "lat": lat[mask],
        "value": vals[mask]
    })

    df.to_parquet(out_path, index=False)
    print(f"✔ Saved Parquet → {out_path}")


# ----------------------------------------------------------------------
# Download + Convert each file
# ----------------------------------------------------------------------
def download_and_convert(blob):
    file_name = blob.name.split("/")[-1]
    var = blob.name.split("/")[1]

    local_tif = f"{LOCAL_TMP}/{file_name}"
    out_parquet = f"{OUT_DIR}/{file_name.replace('.tif', '.parquet')}"

    print(f"⬇ Downloading: {blob.name}")
    blob.download_to_filename(local_tif)

    tif_to_parquet(local_tif, out_parquet)


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    print("🔍 Checking bucket…")

    client = storage.Client()
    bucket = client.bucket(BUCKET)

    blobs = list(bucket.list_blobs(prefix="raw_export/"))

    tif_files = [b for b in blobs if b.name.endswith(".tif")]
    if not tif_files:
        print("⚠ No TIFF files found in bucket.")
        return

    for blob in tif_files:
        download_and_convert(blob)

    print("🎉 All TIFF files converted successfully.")


if __name__ == "__main__":
    main()
