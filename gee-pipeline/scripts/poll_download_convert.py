import os
import pandas as pd
from google.cloud import storage
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa

RAW_OUTPUT = "gee-pipeline/outputs/raw_parquet"
os.makedirs(RAW_OUTPUT, exist_ok=True)

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

# ----------------------------------------
#   DOWNLOAD FILES
# ----------------------------------------
print(f"📥 Downloading files from bucket: {bucket_name}")

blobs = list(bucket.list_blobs(prefix="export/"))

if len(blobs) == 0:
    print("⚠ No files found in export/ path.")
    exit()

for blob in tqdm(blobs, desc="Downloading"):
    if not blob.name.endswith(".csv"):
        continue

    filename = os.path.basename(blob.name)
    local_path = os.path.join(RAW_OUTPUT, filename)

    blob.download_to_filename(local_path)

print("✅ Download complete")

# ----------------------------------------
#   CONVERT TO PARQUET
# ----------------------------------------
print("🔄 Converting CSV → Parquet")

csv_files = [f for f in os.listdir(RAW_OUTPUT) if f.endswith(".csv")]

for csv_file in tqdm(csv_files, desc="Converting"):
    csv_path = os.path.join(RAW_OUTPUT, csv_file)
    df = pd.read_csv(csv_path)

    # Clean column names: lowercase
    df.columns = [col.strip().lower() for col in df.columns]

    # Check required columns
    required_cols = ["province", "district", "subdistrict", "month", "year", "variable"]
    for col in required_cols:
        if col not in df.columns:
            print(f"⚠ Missing column: {col} in {csv_file}")

    # Parquet filename
    parquet_path = csv_path.replace(".csv", ".parquet")

    # Convert
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)

print("🎉 All CSV converted to Parquet successfully")
