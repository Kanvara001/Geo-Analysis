import ee
import datetime
import os

# Initialize
ee.Initialize(project="geo-analysis-472713")

# Shapefile for filtering
fc = ee.FeatureCollection("projects/geo-analysis-472713/assets/shapefile_provinces")

# Variables
VARIABLES = ["NDVI", "LST", "FireCount"]

def get_dataset(variable):
    if variable == "NDVI":
        return ee.ImageCollection("MODIS/061/MOD13A2").select("NDVI")
    if variable == "LST":
        return ee.ImageCollection("MODIS/061/MOD11A2").select("LST_Day_1km")
    if variable == "FireCount":
        return ee.ImageCollection("MODIS/061/MCD14DL").select("Confidence")

def export_monthly(variable, year, month):
    dataset = get_dataset(variable)

    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")

    image = dataset.filterDate(start, end).mean()

    # Correct filename — no .geojson extension
    filename = f"raw_export/{variable}/{variable}_{year}_{month}"

    task = ee.batch.Export.table.toCloudStorage(
        collection=image.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.mean(),
            scale=500
        ),
        description=f"{variable}_{year}_{month}",
        bucket=os.environ.get("GCS_BUCKET"),
        fileNamePrefix=filename,
        fileFormat="GeoJSON"
    )
    task.start()
    print(f"🚀 Export started: {filename}")

def main():
    now = datetime.datetime.utcnow()
    year = now.year
    month = now.month

    for var in VARIABLES:
        export_monthly(var, year, month)

if __name__ == "__main__":
    main()
