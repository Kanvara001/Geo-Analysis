# scripts/gee_export_tasks.py
import ee
import os, json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv("config/template_config.env")

SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT_EMAIL")
KEYPATH = os.getenv("SERVICE_ACCOUNT_KEYPATH")
BUCKET = os.getenv("GCS_BUCKET")
TAMBON_ASSET = os.getenv("TAMBON_ASSET", "projects/geo-analysis-472713/assets/shapefile_provinces")

# Range for backfill (inclusive)
START_YEAR = int(os.getenv("START_YEAR", 2015))
END_YEAR   = int(os.getenv("END_YEAR", 2024))

# WARNING: exporting per-image for long ranges creates many tasks. Batch if needed.
# Set collections configurations
collections = {
    'NDVI': {
        'id': 'MODIS/061/MOD13A2',
        'band': 'NDVI',
        'scale_map': 0.0001  # multiply by this
    },
    'LST': {
        'id': 'MODIS/061/MOD11A2',
        'band': 'LST_Day_1km',
        'scale_map': 0.02
    },
    'RAIN': {
        'id': 'UCSB-CHG/CHIRPS/DAILY',
        'band': 'precipitation',
        'scale_map': 1.0
    },
    'SMAP': {
        'id': 'NASA_USDA/HSL/SMAP10KM_soil_moisture',
        'band': 'ssm',
        'scale_map': 1.0
    },
    'FIRE': {
        # NOTE: you may need to adjust FIRMS id in your GEE environment
        # For FIRE we will export daily point counts via counting points in date window
        'id': 'FIRMS', 
        'band': None,
        'scale_map': 1.0
    }
}

# init EE
credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEYPATH)
ee.Initialize(credentials)

# prepare tambon collection with centroid lon/lat properties
def prepare_tambon_fc(asset_id):
    fc = ee.FeatureCollection(asset_id)
    def add_centroid(f):
        c = f.geometry().centroid(1)
        lon = c.coordinates().get(0)
        lat = c.coordinates().get(1)
        return f.set({'lon': lon, 'lat': lat})
    return fc.map(add_centroid)

TAMBON_FC = prepare_tambon_fc(TAMBON_ASSET)

def export_image_reduce(img, varname, year, month, label):
    # img: ee.Image already scaled and with a band
    # label: unique label (e.g., image id or date)
    desc = f"raw_{varname}_{label}"
    prefix = f"raw/{varname}/{year}/{month:02d}/{desc}"
    task = ee.batch.Export.table.toCloudStorage(
        collection = img.reduceRegions(collection=TAMBON_FC, reducer=ee.Reducer.mean(), scale=1000),
        description = desc,
        bucket = BUCKET,
        fileNamePrefix = prefix,
        fileFormat = 'CSV'
    )
    task.start()
    return {'var': varname, 'desc': desc, 'task_id': task.id, 'prefix': prefix}

tasks = []

# For NDVI/LST/SMAP/RAIN we will iterate images by date
for varname, cfg in collections.items():
    colid = cfg['id']
    band = cfg['band']
    scale = cfg['scale_map']

    if varname == 'FIRE':
        # export counts per tambon per day by iterating daily from START_YEAR-01-01 to END_YEAR-12-31
        # Note: FIRMS is a FeatureCollection of points; we will do filterDate per day and count per tambon
        start = datetime(START_YEAR,1,1)
        end = datetime(END_YEAR,12,31)
        cur = start
        while cur <= end:
            next_day = cur + timedelta(days=1)
            fires = ee.FeatureCollection(collections['FIRE']['id']).filterDate(ee.Date(cur), ee.Date(next_day))
            # create function to count points per tambon and set fire_count prop
            def count_points(f):
                c = fires.filterBounds(f.geometry()).size()
                return f.set('fire_count', c)
            fc = TAMBON_FC.map(count_points)
            # export this day's counts
            label = cur.strftime('%Y%m%d')
            desc = f"raw_FIRE_{label}"
            prefix = f"raw/FIRE/{cur.year}/{cur.month:02d}/{desc}"
            task = ee.batch.Export.table.toCloudStorage(collection=fc, description=desc, bucket=BUCKET, fileNamePrefix=prefix, fileFormat='CSV')
            task.start()
            tasks.append({'var':'FIRE','desc':desc,'task_id':task.id,'prefix':prefix})
            cur = next_day
    else:
        collection = ee.ImageCollection(colid).select(band)
        # iterate by image in full period
        # toList may be large -> process year by year to reduce memory
        for year in range(START_YEAR, END_YEAR+1):
            year_start = ee.Date.fromYMD(year,1,1)
            year_end   = ee.Date.fromYMD(year,12,31).advance(1,'day')
            imgs = collection.filterDate(year_start, year_end).toList(collection.filterDate(year_start, year_end).size())
            n = imgs.size().getInfo()
            for i in range(n):
                img = ee.Image(imgs.get(i))
                # apply scale if necessary
                if scale != 1.0:
                    img = img.multiply(scale)
                # set simple properties
                date = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd')
                img = img.set({'image_date': date})
                label = f"{year}_{i}"
                # reduceRegions will include tambon properties + centroid lon/lat
                task = ee.batch.Export.table.toCloudStorage(
                    collection = img.reduceRegions(collection=TAMBON_FC, reducer=ee.Reducer.mean(), scale=1000),
                    description = f"raw_{varname}_{label}",
                    bucket = BUCKET,
                    fileNamePrefix = f"raw/{varname}/{year}/{int(img.date().get('month')) if False else date.getInfo()}/{varname}_{year}_{i}",
                    fileFormat = 'CSV'
                )
                # Note: using img.date().getInfo() in python-side isn't allowed; instead use date string
                task.start()
                tasks.append({'var':varname,'desc':f"raw_{varname}_{label}",'task_id':task.id,'prefix':f"raw/{varname}/{year}/{date.getInfo()}"})

# save started tasks
os.makedirs('outputs', exist_ok=True)
with open('outputs/raw_export_tasks.json','w') as f:
    json.dump(tasks, f, indent=2)

print("Started total tasks:", len(tasks))
