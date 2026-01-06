import os
import ee
import json
import pandas as pd
import time
from datetime import datetime
import argparse

# -----------------------------
# SAFETY CHECK
# -----------------------------
assert "scripts_auto" in __file__, "❌ Wrong script path"

# -----------------------------
# Paths
# -----------------------------
MERGED_PATH = "gee-pipeline/outputs/merged/merged_dataset.parquet"
RAW_OUTPUT = "raw_export"
BATCH_SIZE = 20

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
# Geometry
# -----------------------------
TAMBON = ee.FeatureCollection(
    "projects/geo-analysis-472713/assets/json_provinces"
)

# -----------------------------
# Dataset definitions (SAME AS MANUAL)
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
        "ic": "NASA/SMAP/SPL4SMGP/007",
        "scale": 10000,
        "reducer": ee.Reducer.mean(),
        "band": "sm_surface",
    },
    "Rainfall": {
        "ic": "UCSB-CHG/CHIRPS/DAILY",
        "scale": 10000,
        "reducer": ee.Reducer.sum(),
        "band": "precipitation",
    },
    "FireCount": {
        "ic": "MODIS/061/MOD14A1",
        "scale": 1000,
        "band": "FireMask",
    },
}

# -----------------------------
# Helpers
# -----------------------------
def month_filter(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    return start, end

def prepare_firecount(img):
    return img.select("FireMask").gte(7).rename("FireCount")

def get_last_year_month():
    if not os.path.exists(MERGED_PATH):
        return 2015, 1

    df = pd.read_parquet(MERGED_PATH)
    df["date"] = pd.to_datetime(df["date"])
    last = df["date"].max()
    return last.year, last.month

def get_months_to_export():
    last_y, last_m = get_last_year_month()
    now = datetime.now()

    months = []
    y, m = last_y, last_m + 1

    while (y < now.year) or (y == now.year and m <= now.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    return months

# -----------------------------
# Export one month
# -----------------------------
def export_month(year, month, variable, spec):

    ic = ee.ImageCollection(spec["ic"]).filterDate(*month_filter(year, month))
    scale = spec["scale"]

    if variable == "FireCount":
        ic = ic.map(prepare_firecount)
        img = ic.sum()
        reducer = ee.Reducer.sum()
    else:
        img = ic.select(spec["band"]).mean()
        reducer = spec["reducer"]

    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=reducer,
        scale=scale,
    )

    zonal = zonal.map(lambda f: f.set({
        "year": year,
        "month": month,
        "variable": variable,
    }))

    filename = f"{variable}_{year}_{month:02d}"

    task = ee.batch.Export.table.toCloudStorage(
        collection=zonal,
        description=filename,
        bucket=os.environ["GCS_BUCKET"],
        fileNamePrefix=f"{RAW_OUTPUT}/{variable}/{filename}",
        fileFormat="GeoJSON",
    )

    task.start()
    return task

# -----------------------------
# Runner
# -----------------------------
def main(var):
    months = get_months_to_export()

    if not months:
        print("✅ No new month to export")
        return

    print(f"🚀 AUTO EXPORT {var}")
    print(f"📅 Months: {months}")

    spec = DATASETS[var]
    count = 0

    for y, m in months:
        task = export_month(y, m, var, spec)
        count += 1

        if count % BATCH_SIZE == 0:
            print("⏳ Waiting for GEE quota…")
            time.sleep(25)

    print(f"🎉 Submitted {count} tasks for {var}")

# -----------------------------
# Entry
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--var", required=True)
    args = parser.parse_args()

    main(args.var)
