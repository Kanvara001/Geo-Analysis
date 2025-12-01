#!/usr/bin/env python3
import ee
import os
from datetime import datetime, timedelta

# -----------------------------------------------------------
# 1) อ่าน Environment Variables (จาก GitHub Actions)
# -----------------------------------------------------------
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT")
GCS_BUCKET = os.getenv("GCS_BUCKET")
KEY_FILE = "gee-pipeline/service-key.json"

if not SERVICE_ACCOUNT:
    raise SystemExit("❌ ERROR: SERVICE_ACCOUNT environment variable missing.")
if not GCS_BUCKET:
    raise SystemExit("❌ ERROR: GCS_BUCKET missing.")

# -----------------------------------------------------------
# 2) Initialize Earth Engine ด้วย Service Account
# -----------------------------------------------------------
credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_FILE)
ee.Initialize(credentials=credentials)
print("✔ Initialized Earth Engine with service account.")

# -----------------------------------------------------------
# 3) กำหนดวันที่แบบเดือนล่าสุด
# -----------------------------------------------------------
today = datetime.utcnow()
first_day = today.replace(day=1)
last_month_end = first_day - timedelta(days=1)
last_month_start = last_month_end.replace(day=1)

START = last_month_start.strftime("%Y-%m-%d")
END   = last_month_end.strftime("%Y-%m-%d")
YEAR  = last_month_start.year
MONTH = last_month_start.month

print(f"🗓 Exporting month: {YEAR}-{MONTH:02d}")

# -----------------------------------------------------------
# 4) ฟังก์ชัน export TIFF → GCS
# -----------------------------------------------------------
def export_tif(image, folder, filename):
    """Export TIFF from EE image to GCS."""
    task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description=f"{folder}-{filename}",
        bucket=GCS_BUCKET,
        fileNamePrefix=f"raw_export/{folder}/{filename}",
        region=image.geometry(),
        scale=1000,
        maxPixels=1e13,
        fileFormat="GeoTIFF"
    )
    task.start()
    print(f"▶ Started export: {folder}/{filename}")

# -----------------------------------------------------------
# 5) Loading Datasets (แบบถูกต้อง)
# -----------------------------------------------------------

# 🌿 NDVI (MODIS MOD13Q1 500m)
NDVI = (
    ee.ImageCollection("MODIS/061/MOD13Q1")
    .filterDate(START, END)
    .select("NDVI")
    .mean()
)

# 🌡 LST (MODIS MOD11A2)
LST = (
    ee.ImageCollection("MODIS/061/MOD11A2")
    .filterDate(START, END)
    .select("LST_Day_1km")
    .mean()
)

# 🌧 Rainfall (CHIRPS Daily)
Rain = (
    ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
    .filterDate(START, END)
    .select("precipitation")
    .sum()
)

# 💧 Soil Moisture (SMAP v008 — ใหม่ล่าสุด)
SM = (
    ee.ImageCollection("NASA/SMAP/SPL4SMGP/008")
    .filterDate(START, END)
    .select("sm_surface")
    .mean()
)

# 🔥 Fire Count (MODIS MCD14DL — FeatureCollection → raster)
FireFC = (
    ee.FeatureCollection("MODIS/061/MCD14DL")
    .filter(ee.Filter.date(START, END))
)

# แปลง point → raster นับจำนวนไฟ
FireRaster = FireFC.reduceToImage(
    properties=["brightness"],          # ใช้ field ที่มีแน่นอน
    reducer=ee.Reducer.count()
)

# -----------------------------------------------------------
# 6) Export ทุกตัวเป็น TIFF
# -----------------------------------------------------------
export_tif(NDVI, "NDVI", f"NDVI_{YEAR}_{MONTH:02d}.tif")
export_tif(LST,  "LST", f"LST_{YEAR}_{MONTH:02d}.tif")
export_tif(Rain, "Rainfall", f"Rainfall_{YEAR}_{MONTH:02d}.tif")
export_tif(SM,   "SoilMoisture", f"SoilMoisture_{YEAR}_{MONTH:02d}.tif")
export_tif(FireRaster, "FireCount", f"FireCount_{YEAR}_{MONTH:02d}.tif")

print("🎉 All export tasks started successfully!")
