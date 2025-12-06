import argparse
from google.cloud import storage
import geopandas as gpd
import pandas as pd
import os
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, default=None, help="Variable folder name to filter")
parser.add_argument("--limit", type=int, default=None)
args = parser.parse_args()

client = storage.Client()
bucket = client.bucket(os.environ["GCS_BUCKET"])

print("📡 Checking Google Cloud Storage...")
blobs = bucket.list_blobs(prefix="raw_export/")
all_files = [b.name for b in blobs if b.name.endswith(".geojson")]

print(f"Found {len(all_files)} files.")

# -----------------------------------------------------
# 🎯 Correct filtering by folder
# -----------------------------------------------------
if args.var:
    folder = f"raw_export/{args.var}/"
    files = [f for f in all_files if f.startswith(folder)]
else:
    files = all_files

if args.limit:
    files = files[:args.limit]

print(f"➡ Will process {len(files)} file(s).")

os.makedirs("gee-pipeline/outputs/raw_parquet", exist_ok=True)

for file_path in tqdm(files):
    print(f"⬇ Downloading {file_path} ...")
    blob = bucket.blob(file_path)
    local_geojson = "temp.geojson"
    blob.download_to_filename(local_geojson)

    gdf = gpd.read_file(local_geojson)

    # add filename as metadata
    gdf["source_file"] = os.path.basename(file_path)

    out_name = os.path.basename(file_path).replace(".geojson", ".parquet")
    gdf.to_parquet(f"gee-pipeline/outputs/raw_parquet/{out_name}")

    os.remove(local_geojson)
