import ee
import json
import os
from datetime import datetime, timedelta

# -----------------------------
# Load service account
# -----------------------------
SERVICE_ACCOUNT = os.environ["SERVICE_ACCOUNT"]
KEYFILE = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEYFILE)
ee.Initialize(credentials)

# -----------------------------
# Load geometry
# -----------------------------
TAMBON = ee.FeatureCollection(
    "projects/geo-analysis-472713/assets/shapefile_provinces"
)

RAW_OUTPUT = "raw_export"

# -----------------------------
# Determine the month to export
# -----------------------------
today = datetime.utcnow().replace(day=1)
last_month = today - timedelta(days=1)

YEAR = last_month.year
MONTH = last_month.month

print(f"📆 Exporting YEAR={YEAR}, MONTH={MONTH}")

# -----------------------------
# Dataset configuration
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
        "ic": "NASA/GPM_L3/IMERG_V07",
        "scale": 10000,
        "reducer": ee.Reducer.sum(),
        "band": "precipitationCal",
    },
    "FireCount": {
        "ic": "MODIS/061/MOD14A1",
        "scale": 1000,
        "reducer": ee.Reducer.count(),
        "band": "FireMask",
    },
}

def month_filter(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    return start, end

def export_month(variable, spec):
    ic = ee.ImageCollection(spec["ic"]).filterDate(*month_filter(YEAR, MONTH))
    img = ic.select(spec["band"]).mean()

    zonal = img.reduceRegions(
        collection=TAMBON,
        reducer=spec["reducer"],
        scale=spec["scale"],
    )

    # ⭐ สำคัญ: map property ชื่อใหม่ให้ตรงกับ clean_raw_data.py
    def rename_props(f):
        return f.set({
            "province": f.get("Province"),
            "amphoe": f.get("District"),
            "tambon": f.get("Subdistric"),
            "year": YEAR,
            "month": MONTH,
            "variable": variable,
        })

    zonal = zonal.map(rename_props)

    filename = f"{variable}_{YEAR}_{MONTH:02d}.geojson"

    task = ee.batch.Export.table.toCloudStorage(
        collection=zonal,
        description=f"{variable}_{YEAR}_{MONTH}",
        bucket=os.environ["GCS_BUCKET"],
        fileNamePrefix=f"{RAW_OUTPUT}/{variable}/{filename}",
        fileFormat="GeoJSON",
    )
    task.start()
    print(f"🚀 Submitted: {variable}_{YEAR}_{MONTH}")

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    for var, spec in DATASETS.items():
        export_month(var, spec)

    print("🎉 All monthly tasks submitted.")
