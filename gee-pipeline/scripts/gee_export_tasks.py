#!/usr/bin/env python3
"""
gee_export_tasks.py
- Initialize Earth Engine with service account key
- For each variable (NDVI, LST, SMAP, RAIN, FIRE) create export tasks per image / per day reducing to tambon FeatureCollection
- Export tables to GCS as CSV under prefixes raw/<VAR>/<YYYY>/<MM>/...
"""

import os
import json
import ee
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------
# Config (from env or config file)
# ---------------------------
load_dotenv(Path(__file__).parents[1] / "config" / "config.env")

SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT") or os.environ.get("GEE_SERVICE_EMAIL")
KEY_PATH = os.environ.get("KEY_PATH", "gee-pipeline/service-key.json")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
TAMBON_ASSET = os.environ.get("TAMBON_ASSET")
START_YEAR = int(os.environ.get("START_YEAR", 2015))
END_YEAR = int(os.environ.get("END_YEAR", 2024))

if not SERVICE_ACCOUNT or not KEY_PATH or not GCS_BUCKET or not TAMBON_ASSET:
    raise ValueError("Please set SERVICE_ACCOUNT, KEY_PATH, GCS_BUCKET, and TAMBON_ASSET (env or config.env)")

# ---------------------------
# Initialize Earth Engine
# ---------------------------
print("Initializing Earth Engine with Service Account:", SERVICE_ACCOUNT)
credentials = ee.ServiceAccountCredentials(SERVICE_ACCOUNT, KEY_PATH)
ee.Initialize(credentials)
print("Earth Engine initialized")

# Load tambon FeatureCollection (should already be a filtered collection for 9 provinces)
tambon_fc = ee.FeatureCollection(TAMBON_ASSET)

# Add lon/lat centroid properties if not present by mapping
def add_centroid_props(f):
    c = f.geometry().centroid(maxError=100)
    lon = ee.Number(c.coordinates().get(0))
    lat = ee.Number(c.coordinates().get(1))
    return f.set({'lon': lon, 'lat': lat})
tambon_fc = tambon_fc.map(add_centroid_props)

# ---------------------------
# Collections settings
# ---------------------------
collections = {
    'NDVI': {
        'id': 'MODIS/061/MOD13A2',
        'band': 'NDVI',
        'scale': 0.0001,   # multiply
        'freq_days': 16
    },
    'LST': {
        'id': 'MODIS/061/MOD11A2',
        'band': 'LST_Day_1km',
        'scale': 0.02,
        'freq_days': 8
    },
    'SMAP': {
        # NOTE: check dataset name available in your EE account. This is a common SMAP product.
        'id': 'NASA_USDA/HSL/SMAP10KM_soil_moisture',
        'band': 'ssm',
        'scale': 1.0,
        'freq_days': 1
    },
    'RAIN': {
        'id': 'UCSB-CHG/CHIRPS/DAILY',
        'band': 'precipitation',
        'scale': 1.0,
        'freq_days': 1
    },
    'FIRE': {
        # placeholder COLLECTION for FIRMS/VIIRS. If your project uses different, replace this id.
        # Many users use "FIRMS" point collections - you may need to update to valid EE collection
        'id': 'FIRMS',   # <- replace if invalid in your account
        'band': None,
        'freq_days': 1
    }
}

# Helper to create export task
def export_table_to_gcs(fc, desc, prefix):
    task = ee.batch.Export.table.toCloudStorage(
        collection=fc,
        description=desc,
        bucket=GCS_BUCKET,
        fileNamePrefix=prefix,
        fileFormat='CSV'
    )
    task.start()
    return task

# Main: iterate per dataset and dates, create tasks
tasks_meta = []
print("Creating export tasks for years", START_YEAR, "to", END_YEAR)

for varname, cfg in collections.items():
    colid = cfg['id']
    band = cfg['band']
    scale = cfg['scale']
    freq = cfg['freq_days']

    print(f"> Processing variable: {varname} (collection: {colid})")

    if varname == 'FIRE':
        # Approach: iterate daily; count points inside each tambon via map
        # Please replace 'FIRMS' with your actual fire points collection id if needed
        for year in range(START_YEAR, END_YEAR + 1):
            start = datetime(year, 1, 1)
            end = datetime(year, 12, 31)
            cur = start
            while cur <= end:
                next_day = cur + timedelta(days=1)
                date_str = cur.strftime('%Y-%m-%d')
                desc = f"raw_FIRE_{date_str}"
                prefix = f"raw/FIRE/{cur.year}/{cur.month:02d}/raw_FIRE_{date_str}"
                try:
                    fires = ee.FeatureCollection(cfg['id']).filterDate(ee.Date(cur), ee.Date(next_day))
                    # Count per tambon: map through tambon_fc and count points within
                    def count_points(feat):
                        c = fires.filterBounds(feat.geometry()).size()
                        return feat.set({'fire_count': c})
                    fc_count = tambon_fc.map(count_points)
                    t = export_table_to_gcs(fc_count, desc, prefix)
                    tasks_meta.append({'var':'FIRE','desc':desc,'prefix':prefix,'date':date_str})
                except Exception as e:
                    print("Error creating FIRE export for", date_str, e)
                cur = next_day
    else:
        # Image-based collections
        col = ee.ImageCollection(colid).select(band)
        # iterate year-month to avoid huge lists
        for year in range(START_YEAR, END_YEAR + 1):
            for month in range(1,13):
                start_date = datetime(year, month, 1)
                if month == 12:
                    end_date = datetime(year+1, 1, 1)
                else:
                    end_date = datetime(year, month+1, 1)
                # filter collection for that month
                ic = col.filterDate(ee.Date(start_date), ee.Date(end_date))
                # toList size
                try:
                    n_images = ic.size().getInfo()
                except Exception as e:
                    print("Could not get size for", varname, year, month, e)
                    n_images = 0
                if n_images == 0:
                    continue
                imgs = ic.toList(n_images)
                for i in range(n_images):
                    try:
                        img = ee.Image(imgs.get(i))
                        # scale band if needed
                        if scale != 1.0:
                            img = img.multiply(scale)
                        date = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd')
                        date_str = date.getInfo()
                        img = img.set({'image_date': date_str})
                        desc = f"raw_{varname}_{date_str}"
                        prefix = f"raw/{varname}/{year}/{month:02d}/raw_{varname}_{date_str}"
                        # reduceRegions
                        reduced = img.reduceRegions(collection=tambon_fc, reducer=ee.Reducer.mean(), scale=1000)
                        t = export_table_to_gcs(reduced, desc, prefix)
                        tasks_meta.append({'var':varname,'desc':desc,'prefix':prefix,'date':date_str})
                    except Exception as e:
                        print("Error scheduling export for", varname, year, month, i, e)

# Save tasks metadata locally (useful for polling)
outdir = Path(__file__).parents[1] / "outputs"
outdir.mkdir(parents=True, exist_ok=True)
with open(outdir / "raw_export_tasks.json", "w") as f:
    json.dump(tasks_meta, f, indent=2)

print("Created", len(tasks_meta), "export tasks (metadata saved to outputs/raw_export_tasks.json)")
print("Note: Check Earth Engine Tasks dashboard for status; use poll script to wait and download.")
