import os
import ee
from datetime import datetime
import time

SERVICE_ACCOUNT = os.environ["SERVICE_ACCOUNT_EMAIL"]
CREDENTIALS = "gee-pipeline/service-key.json"
BUCKET = os.environ["GCS_BUCKET"]

# Authenticate
credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, CREDENTIALS)
ee.Initialize(credentials)
print("Initialized Earth Engine with service account.")

# -------------------------------------------------------------------
# Date parameters
# -------------------------------------------------------------------
now = datetime.utcnow()
YEAR = now.year
MONTH = now.month

# -------------------------------------------------------------------
# Load administrative boundary (Thailand)
# -------------------------------------------------------------------
shp = ee.FeatureCollection("FAO/GAUL_SIMPLIFIED_500m/2015/level2") \
        .filter(ee.Filter.eq("ADM0_NAME", "Thailand"))

# -------------------------------------------------------------------
# VARIABLES
# -------------------------------------------------------------------

def get_ndvi():
    collection = ee.ImageCollection("MODIS/061/MOD13A2") \
        .filterDate(f"{YEAR}-{MONTH:02d}-01",
                    f"{YEAR}-{MONTH:02d}-28") \
        .select("NDVI")
    return collection.mean().rename("NDVI")

def get_lst():
    collection = ee.ImageCollection("MODIS/061/MOD11A2") \
        .filterDate(f"{YEAR}-{MONTH:02d}-01",
                    f"{YEAR}-{MONTH:02d}-28") \
        .select("LST_Day_1km")
    return collection.mean().rename("LST")

def get_rainfall():
    collection = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY") \
        .filterDate(f"{YEAR}-{MONTH:02d}-01",
                    f"{YEAR}-{MONTH:02d}-28")
    return collection.mean().rename("Rainfall")

def get_soil_moisture():
    collection = ee.ImageCollection("NASA/SMAP/SPL4SMGP/007") \
        .filterDate(f"{YEAR}-{MONTH:02d}-01",
                    f"{YEAR}-{MONTH:02d}-28") \
        .select("sm_surface")
    return collection.mean().rename("SoilMoisture")

def get_firecount():
    collection = ee.ImageCollection("MODIS/061/MCD14DL") \
        .filterDate(f"{YEAR}-{MONTH:02d}-01",
                    f"{YEAR}-{MONTH:02d}-28") \
        .select("FireMask")
    return collection.count().rename("FireCount")

VARIABLES = {
    "NDVI": get_ndvi(),
    "LST": get_lst(),
    "Rainfall": get_rainfall(),
    "SoilMoisture": get_soil_moisture(),
    "FireCount": get_firecount(),
}

# -------------------------------------------------------------------
# Export each variable as GeoTIFF
# -------------------------------------------------------------------
def export_tif(img, name):
    out_name = f"{name}/{name}_{YEAR}_{MONTH:02d}.tif"
    task = ee.batch.Export.image.toCloudStorage(
        image=img,
        description=f"{name}_{YEAR}_{MONTH:02d}",
        bucket=BUCKET,
        fileNamePrefix=f"raw_export/{out_name}",
        region=shp.geometry(),
        scale=500,
        maxPixels=1e13,
        fileFormat="GeoTIFF"       # ←♫แก้ตรงนี้สำคัญมาก
    )
    task.start()
    print("Started:", out_name)


# Start all exports
for name, img in VARIABLES.items():
    export_tif(img, name)
    time.sleep(2)
