import ee
import json
import os
import time
from datetime import datetime

# ---------------------------------------------------------
# Load service account & initialize Earth Engine
# ---------------------------------------------------------
SERVICE_ACCOUNT = os.environ["SERVICE_ACCOUNT"]
KEYFILE = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

with open(KEYFILE, "r") as f:
    key_data = json.load(f)

credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEYFILE)
ee.Initialize(credentials)

# ---------------------------------------------------------
# Load geometry
# ---------------------------------------------------------
TAMBON = ee.FeatureCollection(
    "projects/geo-analysis-472713/assets/Provinces"
)

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
RAW_OUTPUT = "raw_export"
BATCH_SIZE = 20

CURRENT_YEAR = datetime.now().year
YEARS = list(range(2015, CURRENT_YEAR + 1))
MONTHS = list(range(1, 13))

# ---------------------------------------------------------
# Dataset configuration
# ---------------------------------------------------------
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
    "Rainfall": {
        "ic": "UCSB-CHG/CHIRPS/DAILY",
        "scale": 10000,
        "reducer": ee.Reducer.sum(),
        "band": "precipitation",
    },
    "SoilMoisture": {
        "ic": "NASA/SMAP/SPL4SMGP/007",
        "scale": 10000,
        "reducer": ee.Reducer.mean(),
        "band": "sm_surface",
    },
    "FireCount": {
        "ic": "MODIS/061/MOD14A1",
        "scale": 1000,
        "band": "FireMask",
    },
}

# ---------------------------------------------------------
# Helper for monthly interval
# ---------------------------------------------------------
def month_filter(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    return start, end

# ---------------------------------------------------------
# Special case for FireCount (count fire pixels)
# ---------------------------------------------------------
def compute_firecount(ic):
    # FireMask: fire pixels = 7,8,9
    fire = ic.select("FireMask") \
             .map(lambda img: img.eq(7).Or(img.eq(8)).Or(img.eq(9)).rename("FirePix"))

    return fire.sum().rename("FireCount")

# ---------------------------------------------------------
# Export function per month per variable
# ---------------------------------------------------------
def export_month(year, month, variable, spec):

    start, end = month_filter(year, month)
    ic = ee.ImageCollection(spec["ic"]).filterDate(start, end)

    # -----------------------------
    # Handle each variable separately
    # -----------------------------
    if variable == "FireCount":
        img = compute_firecount(ic)
        reducer = ee.Reducer.sum()
        band_name = "FireCount"

    else:
        band_name = spec["band"]
        reducer = spec["reducer"]
        img = ic.select(band_name).mean()

    # -----------------------------
    # Zonal statistics
    # -----------------------------
    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=reducer,
        scale=spec["scale"],
        tileScale=4                      # prevent memory error
    )

    # Attach metadata to each feature
    zonal = zonal.map(lambda f: f.set({
        "year": year,
        "month": month,
        "variable": variable
    }))

    # -----------------------------
    # Export task
    # -----------------------------
    filename = f"{variable}_{year}_{month:02d}"

    task = ee.batch.Export.table.toCloudStorage(
        collection=zonal,
        description=f"{variable}_{year}_{month}",
        bucket=os.environ["GCS_BUCKET"],
        fileNamePrefix=f"{RAW_OUTPUT}/{variable}/{filename}",
        fileFormat="GeoJSON",
    )

    task.start()
    return task

# ---------------------------------------------------------
# Run all exports
# ---------------------------------------------------------
def run_all_exports():
    all_tasks = []
    count = 0

    for variable, spec in DATASETS.items():
        for y in YEARS:
            for m in MONTHS:
                print(f"Submitting task → {variable} {y}-{m:02d}")
                task = export_month(y, m, variable, spec)
                all_tasks.append(task)
                count += 1

                if count % BATCH_SIZE == 0:
                    print(f"⏳ Waiting for Earth Engine (batch {count})…")
                    time.sleep(25)

    print(f"🎉 All {len(all_tasks)} tasks submitted.")
    return all_tasks

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting GEE export for NDVI + LST + FireCount + Rainfall + SoilMoisture")
    run_all_exports()
