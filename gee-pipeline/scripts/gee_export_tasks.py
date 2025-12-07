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
# Load geometry
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
    "NDVI": {
        "ic": "MODIS/061/MOD13Q1",
        "scale": 250,
    },
    "LST": {
        "ic": "MODIS/061/MOD11A2",
        "scale": 1000,
    },
    "Rainfall": {
        "ic": "UCSB-CHG/CHIRPS/DAILY",
        "scale": 5000,
    },
    "FireCount": {
        "ic": "MODIS/061/MOD14A1",
        "scale": 1000,
    },
    "SoilMoisture": {  
        "ic": "NASA/SMAP/SPL4SMGP/007",
        "scale": 10000,
    },
}

# -----------------------------
# Helper for monthly interval
# -----------------------------
def month_filter(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    return start, end

# -----------------------------
# Variable-specific image build
# -----------------------------
def build_image(variable, ic):
    if variable == "NDVI":
        # MODIS NDVI → scale factor 0.0001
        return ic.select("NDVI").mean().multiply(0.0001)

    elif variable == "LST":
        # LST in Kelvin → convert to Celsius
        return ic.select("LST_Day_1km").mean().multiply(0.02).subtract(273.15)

    elif variable == "Rainfall":
        # mm per month
        return ic.select("precipitation").sum()

    elif variable == "FireCount":
        # FireMask == 7 means active fire pixel
        fire = ic.select("FireMask").map(lambda x: x.eq(7))
        return fire.sum()

    elif variable == "SoilMoisture":
        # Already scaled
        return ic.select("sm_surface").mean()

    else:
        raise ValueError(f"Unknown variable: {variable}")

# -----------------------------
# Reducer for each variable
# -----------------------------
def get_reducer(variable):
    if variable == "FireCount":
        return ee.Reducer.sum()
    return ee.Reducer.mean()

# -----------------------------
# Export one month of one variable
# -----------------------------
def export_month(year, month, variable, spec):
    ic = ee.ImageCollection(spec["ic"]).filterDate(*month_filter(year, month))
    scale = spec["scale"]

    img = build_image(variable, ic)
    reducer = get_reducer(variable)

    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=reducer,
        scale=scale,
        maxPixels=1e13
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

    for variable, spec in DATASETS.items():
        for y in YEARS:
            for m in MONTHS:

                task = export_month(y, m, variable, spec)
                all_tasks.append(task)
                count += 1

                if count % BATCH_SIZE == 0:
                    print(f"⏳ Waiting 25s … batch {count} submitted")
                    time.sleep(25)

    print(f"🎉 All {len(all_tasks)} tasks submitted.")
    return all_tasks

# -----------------------------
# Run script
# -----------------------------
if __name__ == "__main__":
    print("🚀 Starting GEE export for NDVI + LST + FireCount + Rainfall + SoilMoisture")
    run_all_exports()
