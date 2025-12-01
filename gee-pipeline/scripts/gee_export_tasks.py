import ee
import json
import os
from datetime import datetime

# Load service account key
with open("gee-pipeline/service-key.json") as f:
    key_data = json.load(f)

SERVICE_ACCOUNT = key_data["client_email"]
PROJECT_ID = key_data["project_id"]

ee.Authenticate(credentials=ee.ServiceAccountCredentials(SERVICE_ACCOUNT, "gee-pipeline/service-key.json"))
ee.Initialize(project=PROJECT_ID)

# Study provinces (9 provinces)
PROVINCES = ["Khon Kaen", "Loei", "Udon Thani", "Nong Bua Lamphu",
             "Nakhon Ratchasima", "Buri Ram", "Kalasin",
             "Maha Sarakham", "Chaiyaphum"]

# Variables to export
VARIABLES = {
    "NDVI": {
        "collection": "MODIS/061/MOD13Q1",
        "band": "NDVI"
    },
    "LST": {
        "collection": "MODIS/061/MOD11A2",
        "band": "LST_Day_1km"
    },
    "SoilMoisture": {
        "collection": "NASA_USDA/HSL/SMAP10KM_soil_moisture",
        "band": "ssm"
    },
    "Rainfall": {
        "collection": "UCSB-CHG/CHIRPS/DAILY",
        "band": "precipitation"
    },
    "FireCount": {
        "collection": "FIRMS",
        "band": "fire_mask"
    }
}

# Google Cloud bucket
BUCKET = os.environ.get("GCS_BUCKET")

def get_province_geometry(province):
    """Load Thailand province boundary from your asset path."""
    fc = ee.FeatureCollection("projects/geo-analysis-472713/assets/thailand_provinces")
    return fc.filter(ee.Filter.eq("name", province)).geometry()

def export_month(variable, year, month):
    info = VARIABLES[variable]

    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")

    collection = ee.ImageCollection(info["collection"]) \
        .filterDate(start, end) \
        .select(info["band"]) \
        .mean()

    for province in PROVINCES:
        geom = get_province_geometry(province)

        file_name = f"{variable}_{year}_{month:02d}_{province.replace(' ', '')}"

        task = ee.batch.Export.table.toCloudStorage(
            collection.sampleRegions(
                collection=collection,
                regions=geom,
                scale=1000,
                geometries=True
            ),
            description=file_name,
            bucket=BUCKET,
            fileNamePrefix=f"raw_export/{variable}/{year}/{month:02d}/{file_name}",
            fileFormat="GeoJSON"
        )
        task.start()
        print("🚀 Exporting:", file_name)

def main():
    today = datetime.utcnow()
    year = today.year
    month = today.month - 1
    if month == 0:
        year -= 1
        month = 12

    print(f"📤 Exporting month: {year}-{month:02d}")

    for var in VARIABLES:
        export_month(var, year, month)

if __name__ == "__main__":
    main()
