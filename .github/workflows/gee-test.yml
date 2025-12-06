import os
import json
from google.cloud import storage
import pandas as pd
from tqdm import tqdm

# -----------------------------
# CONFIG
# -----------------------------
bucket_name = os.getenv("GCS_BUCKET")
output_folder = "gee-pipeline/outputs/raw"

if not os.path.exists(output_folder):
    os.makedirs(output_folder, exist_ok=True)

client = storage.Client()
bucket = client.bucket(bucket_name)

# -----------------------------
# LIST FILES
# -----------------------------
print("Listing files in bucket:", bucket_name)
blobs = bucket.list_blobs()

# เลือกเฉพาะไฟล์ .geojson
geojson_files = [b for b in blobs if b.name.endswith(".geojson")]

print(f"Found {len(geojson_files)} .geojson files")

# -----------------------------
# DOWNLOAD + DEBUG + CONVERT
# -----------------------------
for blob in tqdm(geojson_files, desc="Processing files"):
    local_path = os.path.join(output_folder, os.path.basename(blob.name))

    print(f"\nDownloading {blob.name} → {local_path}")
    blob.download_to_filename(local_path)

    # Load GeoJSON
    with open(local_path, "r") as f:
        data = json.load(f)

    features = data.get("features", [])
    if not features:
        print("WARNING: No features found in this file!")
        continue

    # -----------------------------
    # DEBUG PROPERTIES KEYS
    # -----------------------------
    print("DEBUG — Showing first 3 features' property keys:")
    for i, feat in enumerate(features[:3]):
        print(f"  Feature {i} keys → {list(feat.get('properties', {}).keys())}")

    # -----------------------------
    # CONVERT TO PARQUET
    # -----------------------------
    df = pd.json_normalize(features)

    parquet_path = local_path.replace(".geojson", ".parquet")
    df.to_parquet(parquet_path, index=False)

    print(f"Converted → {parquet_path}")
