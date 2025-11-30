import ee
import json
import time

ee.Initialize(project="geo-analysis-472713")

# Load shapefile
fc = ee.FeatureCollection("projects/geo-analysis-472713/assets/shapefile_provinces")

# ---- RENAME RAW FIELDS TO STANDARD NAMES ----
fc = fc.map(lambda f: f
    .set("province", f.get("Province"))
    .set("amphoe", f.get("District"))
    .set("tambon", f.get("Subdistric"))
)

# Export settings
BUCKET = "geo-analysis-472713-bucket"

START_YEAR = 2020
END_YEAR = 2025


def create_monthly_image(year, month):
    return (ee.ImageCollection("MODIS/061/MOD11A2")
            .filterDate(f"{year}-{month:02d}-01", f"{year}-{month:02d}-28")
            .select("LST_Day_1km")
            .mean()
            .multiply(0.02)  # scale factor
            .rename("lst"))


tasks = []

for year in range(START_YEAR, END_YEAR + 1):
    for month in range(1, 13):
        img = create_monthly_image(year, month)

        out_name = f"{year}_{month:02d}.csv"

        task = ee.batch.Export.table.toCloudStorage(
            collection=img.reduceRegions(
                collection=fc,
                reducer=ee.Reducer.mean().combine(
                    reducer2=ee.Reducer.count(),
                    sharedInputs=True
                ),
                scale=1000
            ),
            description=f"export_{year}_{month}",
            bucket=BUCKET,
            fileNamePrefix=f"raw_export/{out_name}",
            fileFormat="CSV"
        )

        task.start()
        tasks.append(task)
        print("Started:", out_name)

print("All tasks started.")
