import os
import json
import pandas as pd
from google.cloud import storage

# -----------------------------
# Config
# -----------------------------
BUCKET_NAME = os.environ["GCS_BUCKET"]
RAW_DIR = "raw_export"

# Output folder must match cleaner script
RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
os.makedirs(RAW_PARQUET_DIR, exist_ok=True)

client = storage.Client()


# -----------------------------
# List all GeoJSON files
# -----------------------------
def list_files():
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=RAW_DIR)
    return [b for b in blobs if b.name.endswith(".geojson")]


# -----------------------------
# Flatten feature safely
# -----------------------------
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


# -----------------------------
# Download + Convert
# -----------------------------
def download_and_convert(files):
    bucket = client.bucket(BUCKET_NAME)

    variable_map = {
        "NDVI": "NDVI",
        "LST": "LST",
        "SoilMoisture": "SoilMoisture",
        "precipitation": "Rainfall",   # <— mapping ใหม่
        "FireCount": "FireCount",
    }

    for blob in files:
        print(f"⬇ Downloading {blob.name} ...")
        content = blob.download_as_text()
        data = json.loads(content)

        rows = []

        for f in data.get("features", []):
            clean = safe_flatten_feature(f)

            variable = None
            value = None

            # Detect variable
            for raw_name, final_name in variable_map.items():
                if raw_name in clean:
                    variable = final_name
                    value = clean[raw_name]
                    break

            if variable is None:
                print("⚠ WARNING: could not find variable in:", clean.keys())
                continue

            clean["variable"] = variable
            clean["value"] = value

            # remove raw variable fields
            for raw_name in variable_map.keys():
                clean.pop(raw_name, None)

            rows.append(clean)

        df = pd.DataFrame(rows)

        # Generate clean filenames
        filename = blob.name.split("/")[-1].replace(".geojson", "")
        local_parquet = f"{RAW_PARQUET_DIR}/{filename}.parquet"
        local_json = f"{RAW_PARQUET_DIR}/{filename}.json"

        # Save JSON (optional)
        with open(local_json, "w") as f:
            json.dump(rows, f)

        # Save Parquet
        print(f"➡ Converting to {local_parquet}")
        df.to_parquet(local_parquet, index=False)


# -----------------------------
# Main
# -----------------------------
def main():
    print("📡 Checking Google Cloud Storage...")
    files = list_files()

    if not files:
        print("⚠ No files found in GCS. Maybe export still running.")
        return

    print(f"Found {len(files)} files.")
    download_and_convert(files)
    print("🎉 Conversion complete!")


if __name__ == "__main__":
    main()
