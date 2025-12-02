import ee
import os
import json
from datetime import datetime

# -----------------------------
# Load service account key
# -----------------------------
KEY_PATH = "gee-pipeline/service-key.json"

with open(KEY_PATH, "r") as f:
    key_data = json.load(f)

SERVICE_ACCOUNT = key_data["client_email"]

# -----------------------------
# Initialize GEE
# -----------------------------
credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_PATH)
ee.Initialize(credentials)


# -----------------------------
# Config
# -----------------------------
YEAR = datetime.utcnow().year
MONTH = datetime.utcnow().month

EXPORT_BUCKET = "geo-analysis-472713-bucket"  # <--- IMPORTANT
BASE_PATH = f"{EXPORT_BUCKET}/raw_export"

VARIABLES = {
    "NDVI": {
        "collection": "MODIS/061/MOD13A2",
        "band": "NDVI"
    },
    "LST": {
        "collection": "MODIS/061/MOD11A2",
        "band": "LST_Day_1km"
    },
    "FireCount": {
        "collection": "MODIS/061/MOD14A1",
        "band": "FireMask"
    }
}

# -----------------------------
# Define region of interest
# -----------------------------
roi = ee.FeatureCollection("FAO/GAUL/2015/level1") \
    .filter(ee.Filter.eq("ADM0_NAME", "Thailand"))


# -----------------------------
# Export function
# -----------------------------
def export_variable(var_name, cfg):
    collection = ee.ImageCollection(cfg["collection"]) \
        .filterDate(f"{YEAR}-{MONTH:02d}-01", f"{YEAR}-{MONTH:02d}-28") \
        .mean() \
        .select(cfg["band"])

    out_name = f"{var_name}_{YEAR}_{MONTH:02d}.geojson"

    task = ee.batch.Export.table.toCloudStorage(
        collection.reduceRegions(
            collection=roi,
            reducer=ee.Reducer.mean(),
            scale=1000
        ),
        description=f"export_{var_name}",
        bucket=EXPORT_BUCKET,
        fileNamePrefix=f"raw_export/{var_name}/{out_name}",
        fileFormat="GeoJSON"
    )

    print(f"📤 Exporting {var_name} → raw_export/{var_name}/{out_name}")
    task.start()


# -----------------------------
# MAIN
# -----------------------------
for var, cfg in VARIABLES.items():
    export_variable(var, cfg)

print("✅ All export tasks started.")
