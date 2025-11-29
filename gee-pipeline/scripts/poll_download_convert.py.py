#!/usr/bin/env python3
"""
poll_download_convert.py
- Poll Earth Engine tasks status (optional)
- List/download CSVs from GCS under raw/ prefix
- Convert each CSV to Parquet and save in gee-pipeline/outputs/raw_parquet/<VAR>/<YYYY>/<MM>/
"""

import os
import time
import json
from pathlib import Path
import pandas as pd
from google.cloud import storage
import ee
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[1] / "config" / "config.env")

SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT") or os.environ.get("GEE_SERVICE_EMAIL")
KEY_PATH = os.environ.get("KEY_PATH", "gee-pipeline/service-key.json")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

if not SERVICE_ACCOUNT or not KEY_PATH or not GCS_BUCKET:
    raise ValueError("Please set SERVICE_ACCOUNT, KEY_PATH, and GCS_BUCKET environment variables")

# Initialize EE and GCS client
credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_PATH)
ee.Initialize(credentials)

from google.oauth2 import service_account
import google.auth.transport.requests

# Build GCS client using the same service account key
sa_creds = service_account.Credentials.from_service_account_file(KEY_PATH)
storage_client = storage.Client(credentials=sa_creds, project=sa_creds.project_id)
bucket = storage_client.bucket(GCS_BUCKET)

OUT_DOWNLOAD = Path(__file__).parents[1] / "outputs" / "raw_download"
OUT_PARQUET = Path(__file__).parents[1] / "outputs" / "raw_parquet"
OUT_DOWNLOAD.mkdir(parents=True, exist_ok=True)
OUT_PARQUET.mkdir(parents=True, exist_ok=True)

# helper to list blobs under prefix
def list_blobs_prefix(prefix):
    blobs = storage_client.list_blobs(GCS_BUCKET, prefix=prefix)
    return [b for b in blobs]

# Wait logic: optional check for running tasks (we will wait until there is at least one CSV in bucket)
print("Polling GCS for exported CSVs under prefix 'raw/' ...")
wait_seconds = 10
max_wait = 60 * 60 * 6  # up to 6 hours
elapsed = 0
found_any = False

while elapsed < max_wait:
    blobs = list_blobs_prefix("raw/")
    csvs = [b for b in blobs if b.name.endswith(".csv") or b.name.endswith(".csv.gz")]
    if len(csvs) > 0:
        print("Found", len(csvs), "csv(s) in bucket.")
        found_any = True
        break
    time.sleep(wait_seconds)
    elapsed += wait_seconds
    print(f"Waiting... {elapsed}s elapsed")

if not found_any:
    print("No CSVs found in bucket after waiting; exiting.")
    exit(0)

# Download CSVs and convert to parquet grouped by variable/year/month
for blob in csvs:
    name = blob.name  # e.g. raw/NDVI/2015/01/raw_NDVI_2015-01-01.csv
    print("Processing blob:", name)
    local_path = OUT_DOWNLOAD / Path(name).name
    blob.download_to_filename(str(local_path))
    print("Downloaded to", local_path)

    try:
        # read csv (GEE CSV often has .geo column; we'll handle)
        df = pd.read_csv(local_path)
    except Exception as e:
        print("Error reading CSV, trying with engine python:", e)
        df = pd.read_csv(local_path, engine='python')

    # derive var/year/month from blob.name
    parts = name.split('/')
    # expecting ['raw', VAR, YYYY, MM, filename]
    if len(parts) < 5:
        print("Unexpected blob path structure:", parts)
        # save to generic folder
        dest_dir = OUT_PARQUET / "UNKNOWN"
    else:
        var = parts[1]
        year = parts[2]
        month = parts[3]
        dest_dir = OUT_PARQUET / var / year / month

    dest_dir.mkdir(parents=True, exist_ok=True)

    # sometimes GEE outputs have columns like 'mean' or the band name; keep geometry columns if any
    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Save as parquet
    out_file = dest_dir / (Path(name).stem + ".parquet")
    df.to_parquet(out_file, index=False)
    print("Wrote parquet:", out_file)

print("All done converting CSVs to parquet.")
