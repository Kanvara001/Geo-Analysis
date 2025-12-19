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
# Load geometry (Tambon)
# -----------------------------
TAMBON = ee.FeatureCollection(
    "projects/geo-analysis-472713/assets/Provinces"
)

# -----------------------------
# Config
# -----------------------------
RAW_OUTPUT = "raw_export"
BATCH_SIZE = 20

CURRENT_YEAR = datetime.now().year
YEARS = list(range(2015, CURRENT_YEAR + 1))
MONTHS = list(range(1, 13))

# -----------------------------
# Dataset definitions
# -----------------------------
DATASETS = {

    # ------------------------------------------------
    # ❌ NDVI (ไม่รันใหม่)
    # ------------------------------------------------
    # "NDVI": {
    #     "ic": "MODIS/061/MOD13Q1",
    #     "scale": 250,
    #     "reducer": ee.Reducer.mean(),
    #     "band": "NDVI",
    # },

    # ------------------------------------------------
    # ❌ LST (ไม่รันใหม่)
    # ------------------------------------------------
    # "LST": {
    #     "ic": "MODIS/061/MOD11A2",
    #     "scale": 1000,
    #     "reducer": ee.Reducer.mean(),
    #     "band": "LST_Day_1km",
    # },

    # ------------------------------------------------
    # ❌ Rainfall (ไม่รันใหม่)
    # ------------------------------------------------
    # "Rainfall": {
    #     "ic": "UCSB-CHG/CHIRPS/DAILY",
    #     "scale": 10000,
    #     "reducer": ee.Reducer.sum(),
    #     "band": "precipitation",
    # },

    # ------------------------------------------------
    # ❌ SoilMoisture (ไม่รันใหม่)
    # ------------------------------------------------
    # "SoilMoisture": {
    #     "ic": "NASA/SMAP/SPL4SMGP/007",
    #     "scale": 10000,
    #     "reducer": ee.Reducer.mean(),
    #     "band": "sm_surface",
    # },

    # ------------------------------------------------
    # ✅ FireCount (รันใหม่เฉพาะตัวนี้)
    # ------------------------------------------------
    "FireCount": {
        "ic": "MODIS/061/MOD14A1",
        "scale": 1000,
        "band": "FireMask",
    },
}

# -----------------------------
# Helper: monthly interval
# -----------------------------
def month_filter(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    return start, end

# -----------------------------
# FireCount preprocessing
# -----------------------------
def prepare_firecount(image):
    """
    FireMask values:
    7 = high-confidence fire

    Output:
    1 = fire detected
    0 = no fire
    """
    fire = image.select("FireMask").eq(7)
    return fire.rename("FireCount")

# -----------------------------
# Export one month
# -----------------------------
def export_month(year, month, variable, spec):
    ic = ee.ImageCollection(spec["ic"]).filterDate(*month_filter(year, month))

    # ------------------------------------------------
    # FireCount logic (FIXED)
    # ------------------------------------------------
    if variable == "FireCount":
        ic = ic.map(prepare_firecount)
        img = ic.sum()  # number of fire days in month

    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=ee.Reducer.sum(),
        scale=spec["scale"],
    )

    zonal = zonal.map(lambda f: f.set({
        "year": year,
        "month": month,
        "variable": variable,
    }))

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

# -----------------------------
# Batch runner
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

    print(f"🎉 Submitted {len(all_tasks)} FireCount export tasks")
    return all_tasks

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    print("🚀 Exporting FireCount ONLY (fixed logic)")
    run_all_exports()
