import ee
import datetime
import time

# ------------------------------
# 1) INITIALIZE SERVICE ACCOUNT
# ------------------------------
SERVICE_ACCOUNT = "YOUR_SERVICE_ACCOUNT@project.iam.gserviceaccount.com"
KEY_FILE = "gee-pipeline/service-key.json"

credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_FILE)
ee.Initialize(credentials)
print("Initialized Earth Engine with service account.")


# ------------------------------
# 2) TIME RANGE (monthly)
# ------------------------------
now = datetime.datetime.utcnow()
year = now.year
month = now.month

start_date = f"{year}-{month:02d}-01"
end_date = f"{year}-{month:02d}-28"  # safe end


# ------------------------------
# 3) COUNTRY SHAPE (Thailand)
# ------------------------------
th = ee.FeatureCollection("FAO/GAUL/2015/level0") \
        .filter(ee.Filter.eq("ADM0_NAME", "Thailand")) \
        .geometry()


# ------------------------------
# 4) DEFINE DATASETS
# ------------------------------

def get_firecount(start, end):
    col = ee.ImageCollection("FIRMS").filterDate(start, end).filterBounds(th)
    return col.count().rename("fire_count")

def get_ndvi(start, end):
    col = ee.ImageCollection("MODIS/061/MOD13Q1").filterDate(start, end)
    img = col.mean()
    return img.select("NDVI").rename("ndvi")

def get_lst(start, end):
    col = ee.ImageCollection("MODIS/061/MOD11A1").filterDate(start, end)
    img = col.mean()
    lst = img.select("LST_Day_1km").multiply(0.02).subtract(273.15)
    return lst.rename("lst")

def get_soilmoisture(start, end):
    col = ee.ImageCollection("NASA_USDA/HSL/SMAP10KM_soil_moisture").filterDate(start, end)
    return col.mean().select("ssm").rename("soil_moisture")

def get_rainfall(start, end):
    col = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate(start, end)
    return col.sum().rename("rainfall")


# ------------------------------
# 5) TASK EXPORT FUNCTION
# ------------------------------
def export_image(image, var_name):
    file_name = f"{var_name}_{year}_{month:02d}"

    task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description=file_name,
        bucket="YOUR_BUCKET_NAME",
        fileNamePrefix=f"raw_export/{var_name}/{file_name}",
        region=th,
        scale=1000,
        maxPixels=1e13,
        fileFormat="GeoJSON"
    )

    task.start()
    print(f"🚀 Started export for {var_name}")

    time.sleep(2)


# ------------------------------
# 6) EXECUTE ALL EXPORTS
# ------------------------------
datasets = {
    "FireCount": get_firecount(start_date, end_date),
    "NDVI": get_ndvi(start_date, end_date),
    "LST": get_lst(start_date, end_date),
    "SoilMoisture": get_soilmoisture(start_date, end_date),
    "Rainfall": get_rainfall(start_date, end_date),
}

for var, img in datasets.items():
    export_image(img, var)

print("🎉 All export tasks started successfully!")
