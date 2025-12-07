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
#   LIST FILES
# ----------------------------------------
blobs = list(bucket.list_blobs(prefix="export/"))

geojson_files = []

for blob in blobs:
    if not blob.name.endswith(".geojson"):
        continue

    filename = os.path.basename(blob.name)
    var = filename.split("_")[0].upper()  # Extract var from filename

    if variable_filter and var != variable_filter:
        continue

    geojson_files.append(blob)

# Apply limit
if limit:
    geojson_files = geojson_files[:limit]

print(f"🔎 Filtered for variable: {variable_filter} → {len(geojson_files)} files")
print(f"🔢 Limit = {limit}")

if len(geojson_files) == 0:
    print("⚠ No matching files found.")
    exit()

# ----------------------------------------
#   DOWNLOAD & CONVERT
# ----------------------------------------
print("⬇ Downloading & converting to Parquet...")

for blob in tqdm(geojson_files, desc="Processing"):
    filename = os.path.basename(blob.name)

    local_geojson_path = os.path.join(RAW_OUTPUT, filename)
    parquet_path = local_geojson_path.replace(".geojson", ".parquet")

    # ---- Download ----
    blob.download_to_filename(local_geojson_path)

    # ---- Convert GEOJSON → Parquet ----
    gdf = gpd.read_file(local_geojson_path)

    # Drop geometry — keep only attributes
    df = pd.DataFrame(gdf.drop(columns="geometry"))

    # Ensure lower-case colnames
    df.columns = [c.lower() for c in df.columns]

    # Save parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)

    # Remove geojson after converting
    os.remove(local_geojson_path)

print("🎉 Conversion complete!")

# ----------------------------------------
#   VERIFY OUTPUT
# ----------------------------------------
parq_list = [f for f in os.listdir(RAW_OUTPUT) if f.endswith(".parquet")]

print(f"📦 Parquet files generated: {len(parq_list)}")
for p in parq_list:
    print(" -", p)

if len(parq_list) == 0:
    raise RuntimeError("❌ No Parquet files were generated!")
