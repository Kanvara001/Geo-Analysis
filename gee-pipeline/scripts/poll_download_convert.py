import argparse
import os
import json
from google.cloud import storage
import pandas as pd

def download_and_convert(var: str, limit: int):
    print(f"📥 Starting poll for variable: {var}, limit = {limit}")

    client = storage.Client()
    bucket_name = os.environ.get("GCS_BUCKET")

    if not bucket_name:
        raise RuntimeError("Missing GCS_BUCKET env variable")

    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    # Filter only <var>_YYYY_MM.geojson
    geojson_files = [
        b for b in blobs
        if b.name.lower().endswith(".geojson")
        and b.name.lower().startswith(f"{var.lower()}_")
    ]

    print(f"🔎 Found {len(geojson_files)} matching geojson files for {var}")

    if len(geojson_files) == 0:
        print("⚠ No matching files found.")
        return

    os.makedirs("raw_geojson", exist_ok=True)
    os.makedirs("raw_parquet", exist_ok=True)

    # Sort newest-first (optional, but more predictable)
    geojson_files = sorted(geojson_files, key=lambda x: x.name, reverse=True)

    for blob in geojson_files[:limit]:
        print(f"⬇ Downloading: {blob.name}")
        local_geojson = f"raw_geojson/{blob.name}"
        blob.download_to_filename(local_geojson)

        print(f"📄 Reading geojson → {local_geojson}")
        with open(local_geojson, "r") as f:
            data = json.load(f)

        # Convert FeatureCollection to DataFrame
        rows = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})
            props["geometry"] = json.dumps(geom)
            rows.append(props)

        df = pd.DataFrame(rows)

        # Save parquet
        parquet_name = blob.name.replace(".geojson", ".parquet")
        output_path = f"raw_parquet/{parquet_name}"

        print(f"💾 Saving parquet → {output_path}")
        df.to_parquet(output_path, index=False)

    print("✅ Conversion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--var", type=str, required=True, help="Variable name (NDVI, LST, SoilMoisture)")
    parser.add_argument("--limit", type=int, default=5, help="Max files to process")

    args = parser.parse_args()
    download_and_convert(args.var, args.limit)
