# === gee_export_tasks.py ===

import ee
import json
import os
import time
from datetime import datetime

# -----------------------------
# Load service account
# -----------------------------
SERVICE_ACCOUNT = os.environ["SERVICE_ACCOUNT"]
KEYFILE = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

with open(KEYFILE, "r") as f:
    key_data = json.load(f)

credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEYFILE)
ee.Initialize(credentials)

# -----------------------------
# Load 9-province tambon geometry
# -----------------------------
TAMBON = ee.FeatureCollection(
    "projects/geo-analysis-472713/assets/shapefile_provinces"
)

# -----------------------------
# Config
# -----------------------------
RAW_OUTPUT = "raw_export"
BATCH_SIZE = 20

YEARS = list(range(2015, 2025))
MONTHS = list(range(1, 13))

# -----------------------------
# Datasets
# -----------------------------
DATASETS = {
    "NDVI": {
        "ic": "MODIS/061/MOD13Q1",
        "scale": 250,
        "reducer": ee.Reducer.mean(),
        "band": "NDVI",
    },
    "LST": {
        "ic": "MODIS/061/MOD11A2",
        "scale": 1000,
        "reducer": ee.Reducer.mean(),
        "band": "LST_Day_1km",
    },
    "SoilMoisture": {
        "ic": "NASA_USDA/HSL/SMAP10KM_soil_moisture",
        "scale": 10000,
        "reducer": ee.Reducer.mean(),
        "band": "ssm",
    },
    "Rainfall": {
        "ic": "NASA/GPM_L3/IMERG_V06",
        "scale": 10000,
        "reducer": ee.Reducer.sum(),
        "band": "precipitationCal",
    },
    "FireCount": {
        "ic": "FIRMS/MODIS/C6_1/FIRE_EVENTS",
        "scale": 1000,
        "reducer": ee.Reducer.count(),
        "band": "brightness",
    },
}

# -----------------------------
# Helper to create monthly filter
# -----------------------------
def month_filter(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    return start, end


# -----------------------------
# Export function
# -----------------------------
def export_month(year, month, variable, spec):
    ic = ee.ImageCollection(spec["ic"]).filterDate(*month_filter(year, month))

    band = spec["band"]
    reducer = spec["reducer"]
    scale = spec["scale"]

    img = ic.select(band).mean()

    # Zonal statistics
    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=reducer,
        scale=scale,
    )

    # Add metadata
    zonal = zonal.map(
        lambda f: f.set({
            "year": year,
            "month": month,
            "variable": variable,
        })
    )

    # Export
    filename = f"{variable}_{year}_{month:02d}.geojson"

    task = ee.batch.Export.table.toCloudStorage(
        collection=zonal,
        description=f"{variable}_{year}_{month}",
        bucket=os.environ["GCS_BUCKET"],
        fileNamePrefix=f"{RAW_OUTPUT}/{variable}/{filename}",
        fileFormat="GeoJSON",
    )
    task.start()
    return task


# -----------------------------
# Run exports in batch
# -----------------------------
def run_all_exports():
    all_tasks = []
    count = 0

    for var, spec in DATASETS.items():
        for y in YEARS:
            for m in MONTHS:

                task = export_month(y, m, var, spec)
                all_tasks.append(task)
                count += 1

                if count % BATCH_SIZE == 0:
                    print(f"⏳ Waiting for GEE… batch {count} submitted")
                    time.sleep(25)

    print(f"🎉 All {len(all_tasks)} tasks submitted.")
    return all_tasks


# -----------------------------
# Run script
# -----------------------------
if __name__ == "__main__":
    print("🚀 Starting GEE exports (batch mode)…")
    run_all_exports()
