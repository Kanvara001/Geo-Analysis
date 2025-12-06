import argparse
import json
import os
from google.cloud import storage
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
os.makedirs(RAW_PARQUET_DIR, exist_ok=True)

# -----------------------------
# 📌 CLI arguments
# -----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, default=None, help="Variable to filter (e.g., NDVI, LST)")
parser.add_argument("--limit", type=int, default=None, help="Limit number of files to download")
args = parser.parse_args()

# -----------------------------
# 📌 Connect GCS
# -----------------------------
client = storage.Client()
bucket = client.bucket(os.environ["GCS_BUCKET"])

print("📡 Checking Google Cloud Storage...")

# List blobs
blobs = list(client.list_blobs(os.environ["GCS_BUCKET"], prefix="raw_export/"))
print(f"Found {len(blobs)} files.")

# -----------------------------
# 📌 Apply VAR FILTER
# -----------------------------
if args.var:
    blobs = [b for b in blobs if f"/{args.var}/" in b.name]
    print(f"🔍 Filtered by variable={args.var} → {len(blobs)} files")

# -----------------------------
# 📌 Apply LIMIT
# -----------------------------
if args.limit:
    blobs = blobs[: args.limit]
    print(f"🔽 Using limit={args.limit} → processing first {len(blobs)} files")

# -----------------------------
# 📌 Process each file
# -----------------------------
for blob in blobs:
    print(f"⬇ Downloading {blob.name} ...")

    local_json = f"{RAW_PARQUET_DIR}/{os.path.basename(blob.name)}"
    blob.download_to_filename(local_json)

    # Load geojson
    try:
        gdf = gpd.read_file(local_json)
    except Exception as e:
        print(f"❌ Error reading {local_json}: {e}")
        continue

    # Normalize schema (fix inconsistent naming)
    rename_map = {
        "Province": "province",
        "District": "amphoe",
        "Subdistric": "tambon",
        "month": "month",
        "year": "year",
        "variable": "variable",
        "mean": "value",
        "sum": "value",  # FireCount uses "sum"
    }
    gdf = gdf.rename(columns={k: v for k, v in rename_map.items() if k in gdf.columns})

    # Check required fields
    required = ["province", "amphoe", "tambon", "variable", "year", "month", "value"]
    missing = [c for c in required if c not in gdf.columns]

    if missing:
        print(f"⚠ WARNING: missing fields: {missing}")
        continue

    # Save parquet
    filename = os.path.basename(blob.name).replace(".geojson", ".parquet")
    output_parquet = f"{RAW_PARQUET_DIR}/{filename}"
    gdf[required].to_parquet(output_parquet, index=False)

    print(f"✔ Saved → {output_parquet}")
