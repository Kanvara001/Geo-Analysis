import os
import json
import time
import ee
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# =====================================================
# 🔐 AUTHENTICATION (FIXED – SERVICE ACCOUNT ONLY)
# =====================================================
SERVICE_ACCOUNT = os.environ["SERVICE_ACCOUNT"]
KEYFILE = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

credentials = ee.ServiceAccountCredentials(
    SERVICE_ACCOUNT,
    KEYFILE
)
ee.Initialize(credentials)
print("✅ Earth Engine initialized with service account")

# =====================================================
# 📁 PATH CONFIG
# =====================================================
MERGED_PATH = "gee-pipeline/outputs/merged/merged_dataset.parquet"
RAW_OUTPUT = "raw"

# =====================================================
# 🗺 LOAD GEOMETRY
# =====================================================
TAMBON = ee.FeatureCollection(
    "projects/geo-analysis-472713/assets/json_provinces"
)

# =====================================================
# 📊 DATASETS CONFIG
# =====================================================
DATASETS = {
    "NDVI": {
        "ic": "MODIS/061/MOD13Q1",
        "band": "NDVI",
        "scale": 250,
        "reducer": ee.Reducer.mean(),
    },
    "LST": {
        "ic": "MODIS/061/MOD11A2",
        "band": "LST_Day_1km",
        "scale": 1000,
        "reducer": ee.Reducer.mean(),
    },
    "SOILMOISTURE": {
        "ic": "NASA/SMAP/SPL4SMGP/007",
        "band": "sm_surface",
        "scale": 10000,
        "reducer": ee.Reducer.mean(),
    },
    "RAINFALL": {
        "ic": "UCSB-CHG/CHIRPS/DAILY",
        "band": "precipitation",
        "scale": 10000,
        "reducer": ee.Reducer.sum(),
    },
    "FIRECOUNT": {
        "ic": "MODIS/061/MOD14A1",
        "band": "FireMask",
        "scale": 1000,
    },
}

# =====================================================
# 🔥 FIRE PREPROCESS
# =====================================================
def prepare_fire(img):
    return img.select("FireMask").gte(7).rename("FIRECOUNT")

# =====================================================
# 📅 FIND NEXT MONTH (INCREMENTAL)
# =====================================================
def get_next_month():
    if not os.path.exists(MERGED_PATH):
        raise RuntimeError("❌ merged_dataset.parquet not found")

    df = pd.read_parquet(MERGED_PATH)

    last_year = int(df["year"].max())
    last_month = int(df[df["year"] == last_year]["month"].max())

    target = datetime(last_year, last_month, 1) + relativedelta(months=1)
    today = datetime.today()

    if target.year == today.year and target.month == today.month:
        print("⏸ Current month not finished yet")
        return None

    return target.year, target.month

# =====================================================
# 🚀 EXPORT ONE MONTH
# =====================================================
def export_month(year, month, var, spec):

    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")

    ic = ee.ImageCollection(spec["ic"]).filterDate(start, end)

    if var == "FIRECOUNT":
        ic = ic.map(prepare_fire)
        img = ic.sum()
        reducer = ee.Reducer.sum()
    else:
        img = ic.select(spec["band"]).mean()
        reducer = spec["reducer"]

    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=reducer,
        scale=spec["scale"],
    )

    zonal = zonal.map(lambda f: f.set({
        "year": year,
        "month": month,
        "variable": var,
    }))

    filename = f"{var}_{year}_{month:02d}"

    task = ee.batch.Export.table.toCloudStorage(
        collection=zonal,
        description=filename,
        bucket=os.environ["GCS_BUCKET"],
        fileNamePrefix=f"{RAW_OUTPUT}/{var}/{filename}",
        fileFormat="GeoJSON",
    )

    task.start()
    print(f"🚀 Export started: {filename}")
    return task

# =====================================================
# ▶ MAIN
# =====================================================
def main():
    target = get_next_month()
    if target is None:
        return

    year, month = target
    print(f"📅 EXPORT TARGET: {year}-{month:02d}")

    tasks = []
    for var, spec in DATASETS.items():
        task = export_month(year, month, var, spec)
        tasks.append(task)
        time.sleep(5)

    print(f"✅ Submitted {len(tasks)} export tasks")

if __name__ == "__main__":
    main()
