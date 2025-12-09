import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import geopandas as gpd
from google.cloud import storage
from tqdm import tqdm

RAW_OUTPUT = "gee-pipeline/outputs/raw_parquet"
os.makedirs(RAW_OUTPUT, exist_ok=True)

# ----------------------------------------
#   ARGUMENT PARSER
# ----------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, default=None, help="Filter variable (NDVI, LST, Rainfall, SoilMoisture, FireCount)")
parser.add_argument("--limit", type=int, default=None, help="Limit number of downloaded files")
args = parser.parse_args()

variable_filter = args.var.upper() if args.var else None
limit = args.limit

# ----------------------------------------
#   LOAD ENV
# ----------------------------------------
bucket_name = os.getenv("GCS_BUCKET")
if not bucket_name:
    raise ValueError("❌ ERROR: GCS_BUCKET is not set in environment variables!")

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_path:
    raise ValueError("❌ ERROR: GOOGLE_APPLICATION_CREDENTIALS is not set!")

# ----------------------------------------
#   INIT CLIENT
# ----------------------------------------
client = storage.Client.from_service_account_json(credentials_path)
bucket = client.bucket(bucket_name)

print(f"📥 Downloading from bucket: {bucket_name}")

# ----------------------------------------
#   LIST FILES (UPDATED PREFIX)
# ----------------------------------------
print("🔎 Scanning bucket for GeoJSON files...")

blobs = list(bucket.list_blobs(prefix="raw_export/"))

geojson_files = []

for blob in blobs:
    if not blob.name.endswith(".geojson"):
        continue

    # Extract variable name from folder structure
    parts = blob.name.split("/")
    if len(parts) < 3:
        continue

    var = parts[1].upper()

    if variable_filter and var != variable_filter:
        continue

    geojson_files.append((blob, var))

# Apply limit
if limit:
    geojson_files = geojson_files[:limit]

print(f"🔎 Filtered for variable: {variable_filter} → {len(geojson_files)} files")
print(f"🔢 Limit = {limit}")

if len(geojson_files) == 0:
    print("⚠ No matching files found in raw_export/")
    exit()

# ----------------------------------------
#   DOWNLOAD → CONVERT → UPLOAD BACK TO GCS
# ----------------------------------------
print("⬇ Downloading → 🔄 Converting → ⬆ Uploading Parquet to GCS")

for blob, var in tqdm(geojson_files, desc="Processing"):
    filename = os.path.basename(blob.name)
    local_geojson_path = os.path.join(RAW_OUTPUT, filename)
    parquet_filename = filename.replace(".geojson", ".parquet")
    parquet_path = os.path.join(RAW_OUTPUT, parquet_filename)

    # ---- Download ----
    blob.download_to_filename(local_geojson_path)

    # ---- Convert GEOJSON → Parquet ----
    gdf = gpd.read_file(local_geojson_path)
    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df.columns = [c.lower() for c in df.columns]

    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)

    # ---- Upload Parquet → GCS ----
    gcs_output_path = f"parquet/{var}/{parquet_filename}"
    out_blob = bucket.blob(gcs_output_path)
    out_blob.upload_from_filename(parquet_path)

    print(f"✔ Uploaded: gs://{bucket_name}/{gcs_output_path}")

    # Clean local files
    os.remove(local_geojson_path)
    os.remove(parquet_path)

print("🎉 Conversion + Upload complete!")
