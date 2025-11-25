# scripts/gee_export_tasks.py
import ee
import os
import json
from datetime import datetime
from dotenv import load_dotenv


load_dotenv("config/template_config.env")


SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT_EMAIL")
KEY = os.getenv("SERVICE_ACCOUNT_KEYPATH")
BUCKET = os.getenv("GCS_BUCKET")

# Use the provided GEE asset for tambon-level polygons
TAMBON_ASSET = os.getenv("TAMBON_ASSET", "projects/geo-analysis-472713/assets/shapefile_provinces")
EXPORT_PREFIX = "monthly"


YEAR = int(os.getenv("EXPORT_YEAR", datetime.today().year))
MONTH = int(os.getenv("EXPORT_MONTH", datetime.today().month))


credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY)
ee.Initialize(credentials)

# --------------------------
# Export Functions
# --------------------------
def export_ndvi(year, month):
start = ee.Date.fromYMD(year, month, 1)
end = start.advance(1, 'month')


col = ee.ImageCollection("MODIS/061/MOD13A2").select("NDVI") \
.filterDate(start, end).map(lambda i: i.multiply(0.0001))


img = col.mean().set({'year': year, 'month': month})


fc = img.reduceRegions(
collection=ee.FeatureCollection(TAMBON_ASSET),
reducer=ee.Reducer.mean(),
scale=1000
)


desc = f"NDVI_{year}_{month:02d}"
task = ee.batch.Export.table.toCloudStorage(
collection=fc,
description=desc,
bucket=BUCKET,
fileNamePrefix=f"{EXPORT_PREFIX}/{desc}",
fileFormat="CSV"
)
task.start()
return desc, task.id

def export_lst(year, month):
start = ee.Date.fromYMD(year, month, 1)
end = start.advance(1, 'month')


col = ee.ImageCollection("MODIS/061/MOD11A2").select("LST_Day_1km") \
.filterDate(start, end).map(lambda i: i.multiply(0.02))


img = col.mean().set({'year': year, 'month': month})


fc = img.reduceRegions(
collection=ee.FeatureCollection(TAMBON_ASSET),
reducer=ee.Reducer.mean(),
scale=1000
)


desc = f"LST_{year}_{month:02d}"
task = ee.batch.Export.table.toCloudStorage(
collection=fc,
description=desc,
bucket=BUCKET,
fileNamePrefix=f"{EXPORT_PREFIX}/{desc}",
fileFormat="CSV"
)
task.start()
return desc, task.id

def export_rain(year, month):
start = ee.Date.fromYMD(year, month, 1)
end = start.advance(1, 'month')


col = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").select("precipitation").filterDate(start, end)
img = col.sum().set({'year': year, 'month': month})


fc = img.reduceRegions(
collection=ee.FeatureCollection(TAMBON_ASSET),
reducer=ee.Reducer.sum(),
scale=5000
)


desc = f"RAIN_{year}_{month:02d}"
task = ee.batch.Export.table.toCloudStorage(
collection=fc,
description=desc,
bucket=BUCKET,
fileNamePrefix=f"{EXPORT_PREFIX}/{desc}",
fileFormat="CSV"
)
task.start()
return desc, task.id

def export_smap(year, month):
start = ee.Date.fromYMD(year, month, 1)
end = start.advance(1, 'month')


col = ee.ImageCollection("NASA_USDA/HSL/SMAP10KM_soil_moisture").select("ssm") \
.filterDate(start, end)


img = col.mean().set({'year': year, 'month': month})


fc = img.reduceRegions(
collection=ee.FeatureCollection(TAMBON_ASSET),
reducer=ee.Reducer.mean(),
scale=10000
)


desc = f"SMAP_{year}_{month:02d}"
task = ee.batch.Export.table.toCloudStorage(
collection=fc,
description=desc,
bucket=BUCKET,
fileNamePrefix=f"{EXPORT_PREFIX}/{desc}",
fileFormat="CSV"
)
task.start()
return desc, task.id

def export_fire(year, month):
start = ee.Date.fromYMD(year, month, 1)
end = start.advance(1, 'month')


# Use FIRMS point collection (ensure correct collection ID in your GEE environment)
fires = ee.FeatureCollection("FIRMS").filterDate(start, end)


def count_points(f):
c = fires.filterBounds(f.geometry()).size()
return f.set('fire_count', c)


fc = ee.FeatureCollection(TAMBON_ASSET).map(count_points)


desc = f"FIRE_{year}_{month:02d}"
task = ee.batch.Export.table.toCloudStorage(
collection=fc,
description=desc,
bucket=BUCKET,
fileNamePrefix=f"{EXPORT_PREFIX}/{desc}",
fileFormat="CSV"
)
task.start()
return desc, task.id

# --------------------------
# Run exports
# --------------------------
if __name__ == "__main__":
tasks = []


for fn in [export_ndvi, export_lst, export_rain, export_smap, export_fire]:
desc, tid = fn(YEAR, MONTH)
tasks.append({"desc": desc, "task_id": tid})


os.makedirs('outputs', exist_ok=True)
with open("outputs/gee_tasks.json", "w") as f:
json.dump({"year": YEAR, "month":
