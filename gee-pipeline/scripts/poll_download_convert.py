import os
import json
import argparse
from google.cloud import storage
import pandas as pd

BUCKET_NAME = os.environ["GCS_BUCKET"]
RAW_DIR = "raw_export"
RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"

os.makedirs(RAW_PARQUET_DIR, exist_ok=True)

client = storage.Client()

def list_files(variable=None):
    bucket = client.bucket(BUCKET_NAME)

    prefix = RAW_DIR + "/"
    if variable:
        prefix = f"{RAW_DIR}/{variable}/"

    blobs = bucket.list_blobs(prefix=prefix)
    return [b for b in blobs if b.name.endswith(".geojson")]

def safe_flatten_feature(f):
    props = f.get("properties", {})
    props.pop("geometry", None)
    f.pop("geometry", None)

    clean = {}
    for k, v in props.items():
        if isinstance(v, (list, dict)):
            continue
        clean[k] = v
    return clean

def download_and_convert(files):
    for blob in files:
        print(f"⬇ Downloading {blob.name} ...")
        content = blob.download_as_text()
        data = json.loads(content)
        rows = [safe_flatten_feature(f) for f in data.get("features", [])]
        df = pd.DataFrame(rows)

        filename = blob.name.split("/")[-1].replace(".geojson", "")
        local_parquet = f"{RAW_PARQUET_DIR}/{filename}.parquet"
        local_json = f"{RAW_PARQUET_DIR}/{filename}.json"

        with open(local_json, "w") as f:
            json.dump(rows, f)

        print(f"➡ Converting to {local_parquet}")
        df.to_parquet(local_parquet, index=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--var", type=str, default=None)
    parser.add_argument("--limit", type=int, default=9999999)
    args = parser.parse_args()

    print("📡 Checking Google Cloud Storage...")
    files = list_files(args.var)

    if not files:
        print("⚠ No files found")
        return

    print(f"Found {len(files)} files")
    files = files[: args.limit]

    download_and_convert(files)
    print("🎉 Conversion complete!")

if __name__ == "__main__":
    main()
