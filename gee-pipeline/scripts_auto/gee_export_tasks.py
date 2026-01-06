import os
import ee
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

assert "scripts_auto" in __file__, "❌ Wrong script path"

MERGED_PATH = "gee-pipeline/outputs/merged/merged_dataset.parquet"

# -----------------------------
# Load last available month
# -----------------------------
def get_next_month():
    df = pd.read_parquet(MERGED_PATH)
    df["date"] = pd.to_datetime(df["date"])
    last_date = df["date"].max()

    target = last_date + relativedelta(months=1)

    # ถ้าเดือนนั้นยังไม่ปิด → ไม่ทำ
    today = datetime.today()
    if target.year == today.year and target.month == today.month:
        print("⏸ Month not completed yet")
        return None

    return target.year, target.month

# -----------------------------
# GEE auth
# -----------------------------
SERVICE_ACCOUNT = os.environ["SERVICE_ACCOUNT"]
KEYFILE = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEYFILE)
ee.Initialize(credentials)

# -----------------------------
# Geometry
# -----------------------------
TAMBON = ee.FeatureCollection(
    "projects/geo-analysis-472713/assets/json_provinces"
)

# -----------------------------
# Dataset definitions (เหมือน manual)
# -----------------------------
DATASETS = {
    "NDVI": {
        "ic": "MODIS/061/MOD13Q1",
        "scale": 250,
        "band": "NDVI",
        "reducer": ee.Reducer.mean(),
    },
    "LST": {
        "ic": "MODIS/061/MOD11A2",
        "scale": 1000,
        "band": "LST_Day_1km",
        "reducer": ee.Reducer.mean(),
    },
    "SoilMoisture": {
        "ic": "NASA/SMAP/SPL4SMGP/007",
        "scale": 10000,
        "band": "sm_surface",
        "reducer": ee.Reducer.mean(),
    },
    "Rainfall": {
        "ic": "UCSB-CHG/CHIRPS/DAILY",
        "scale": 10000,
        "band": "precipitation",
        "reducer": ee.Reducer.sum(),
    },
    "FireCount": {
        "ic": "MODIS/061/MOD14A1",
        "scale": 1000,
        "band": "FireMask",
    },
}

# -----------------------------
def month_filter(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    return start, end

def prepare_firecount(img):
    return img.select("FireMask").gte(7).rename("FireCount")

# -----------------------------
def export_month(year, month, var, spec):
    ic = ee.ImageCollection(spec["ic"]).filterDate(*month_filter(year, month))

    if var == "FireCount":
        ic = ic.map(prepare_firecount)
        img = ic.sum()
        reducer = ee.Reducer.sum()
    else:
        img = ic.select(spec["band"]).mean()
        reducer = spec["reducer"]

    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=reducer,
        scale=spec["scale"],
    ).map(lambda f: f.set({
        "year": year,
        "month": month,
        "variable": var
    }))

    task = ee.batch.Export.table.toCloudStorage(
        collection=zonal,
        description=f"{var}_{year}_{month}",
        bucket=os.environ["GCS_BUCKET"],
        fileNamePrefix=f"raw_export/{var}/{var}_{year}_{month:02d}",
        fileFormat="GeoJSON",
    )

    task.start()
    print(f"🚀 Submitted {var} {year}-{month:02d}")

# -----------------------------
def main():
    target = get_next_month()
    if target is None:
        return

    year, month = target
    print(f"📅 EXPORT TARGET: {year}-{month:02d}")

    for var, spec in DATASETS.items():
        export_month(year, month, var, spec)

if __name__ == "__main__":
    main()
