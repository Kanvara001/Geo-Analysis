import os
import json
import pandas as pd
from google.cloud import storage

BUCKET_NAME = os.environ["GCS_BUCKET"]

# -----------------------------
# Function: Flatten GeoJSON
# -----------------------------
def geojson_to_dataframe(path):
    """
    Load a GeoJSON file and flatten features into a clean DataFrame.
    Each row = 1 feature.
    """

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_rows = []

    for feature in data.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})

        row = {}

        # Add all properties normally
        for k, v in props.items():
            row[k] = v

        # Geometry handling (store geometry as WKT string)
        if geom.get("type") == "Point":
            coords = geom["coordinates"]
            row["lon"] = coords[0]
            row["lat"] = coords[1]
        else:
            # If polygon or others, save JSON as text
            row["geometry_json"] = json.dumps(geom)

        all_rows.append(row)

    return pd.DataFrame(all_rows)


# -----------------------------
# Function: Download + Convert
# -----------------------------
def download_and_convert(blob):
    local_geojson = f"/tmp/{blob.name.replace('/', '_')}"
    blob.download_to_filename(local_geojson)

    print(f"⬇ Downloaded: {blob.name}")

    # Convert to DataFrame
    df = geojson_to_dataframe(local_geojson)

    # Ensure output folder exists
    os.makedirs("gee-pipeline/outputs/raw_parquet", exist_ok=True)

    out_name = blob.name.split("/")[-1].replace(".geojson", "").replace(".geojson", "")
    parquet_path = f"gee-pipeline/outputs/raw_parquet/{out_name}.parquet"

    df.to_parquet(parquet_path, index=False)
    print(f"✔ Converted → {parquet_path}")


# -----------------------------
# Main
# -----------------------------
def main():
    print("🔍 Checking bucket…")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = list(bucket.list_blobs(prefix="raw_export/"))

    targets = [b for b in blobs if b.name.endswith(".geojson.geojson")]

    if not targets:
        print("⚠ No GeoJSON found. Nothing to convert.")
        return

    for blob in targets:
        download_and_convert(blob)


if __name__ == "__main__":
    main()
