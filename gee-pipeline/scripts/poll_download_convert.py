import os
import argparse
import pandas as pd
from google.cloud import storage
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa

RAW_OUTPUT = "gee-pipeline/outputs/raw_parquet"
os.makedirs(RAW_OUTPUT, exist_ok=True)

# --------------------------
# Args
# --------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, help="Filter variable name (NDVI, LST, Rainfall, ...)")
parser.add_argument("--limit", type=int, help="Limit number of files")
args = parser.parse_args()

# --------------------------
# Load ENV
# --------------------------
bucket_name = os.getenv("GCS_BUCKET")
if not bucket_name:
    raise ValueError("❌ GCS_BUCKET not set")

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_path:
    raise ValueError("❌ GOOGLE_APPLICATION_CREDENTIALS not set")

# --------------------------
# Init client
# --------------------------
client = storage.Client.from_service_account_json(credentials_path)
bucket = client.bucket(bucket_name)

# --------------------------
# List blobs
# --------------------------
print(f"📥 Downloading from bucket: {bucket_name}")

blobs = list(bucket.list_blobs(prefix="export/"))

# Filter by var
if args.var:
    blobs = [b for b in blobs if args.var.upper() in b.name.upper()]
    print(f"🔎 Filtered for variable: {args.var} → {len(blobs)} files")

# Apply limit
if args.limit:
    blobs = blobs[:args.limit]
    print(f"🔢 Limit = {args.limit}")

if len(blobs) == 0:
    print("⚠ No matching files found.")
    exit(0)

# --------------------------
# Download
# --------------------------
for blob in tqdm(blobs, desc="Downloading"):
    if not blob.name.endswith(".csv"):
        continue

    local_path = os.path.join(RAW_OUTPUT, os.path.basename(blob.name))
    blob.download_to_filename(local_path)

# --------------------------
# Convert CSV → Parquet
# --------------------------
csv_files = [f for f in os.listdir(RAW_OUTPUT) if f.endswith(".csv")]

for csv_file in tqdm(csv_files, desc="Converting"):
    df = pd.read_csv(os.path.join(RAW_OUTPUT, csv_file))
    df.columns = [c.strip().lower() for c in df.columns]

    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(RAW_OUTPUT, csv_file.replace(".csv", ".parquet")))

    os.remove(os.path.join(RAW_OUTPUT, csv_file))

print("🎉 Convert OK")
