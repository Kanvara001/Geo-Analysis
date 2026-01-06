import ee
import pandas as pd
from pathlib import Path
from datetime import datetime
import os
import sys

# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------
SUMMARY_CSV = Path("data/processed/gee_monthly_summary.csv")
ASSET_ROOT = "projects/geo-analysis-472713/assets/monthly"

SERVICE_ACCOUNT = os.getenv(
    "GEE_SERVICE_ACCOUNT",
    "gee-runner@geo-analysis-472713.iam.gserviceaccount.com"
)

KEY_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ----------------------------------------------------
# GEE INIT
# ----------------------------------------------------
if KEY_FILE is None or not Path(KEY_FILE).exists():
    raise FileNotFoundError(
        "GOOGLE_APPLICATION_CREDENTIALS not found. "
        "Did you set it in GitHub Actions?"
    )

credentials = ee.ServiceAccountCredentials(
    SERVICE_ACCOUNT,
    KEY_FILE
)
ee.Initialize(credentials)

# ----------------------------------------------------
# UTILS
# ----------------------------------------------------
def get_next_month():
    if not SUMMARY_CSV.exists():
        today = datetime.today()
        return today.year, today.month

    df = pd.read_csv(SUMMARY_CSV)

    required_cols = {"year", "month"}
    if not required_cols.issubset(df.columns):
        raise ValueError(
            f"Missing columns: {required_cols - set(df.columns)}"
        )

    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    df["date"] = pd.to_datetime(
        dict(year=df.year, month=df.month, day=1)
    )

    latest = df["date"].max()
    next_month = latest + pd.DateOffset(months=1)

    return next_month.year, next_month.month


def build_asset_id(var, year, month):
    mm = str(month).zfill(2)
    return f"{ASSET_ROOT}/{var}_{year}_{mm}"


def export_task(image, asset_id, region):
    task = ee.batch.Export.image.toAsset(
        image=image,
        description=asset_id.split("/")[-1],
        assetId=asset_id,
        region=region,
        scale=1000,
        maxPixels=1e13
    )
    task.start()
    print(f"🚀 Export started: {asset_id}")


# ----------------------------------------------------
# MAIN
# ----------------------------------------------------
def main():

    year, month = get_next_month()
    print(f"📅 Target month: {year}-{str(month).zfill(2)}")

    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")

    provinces = ee.FeatureCollection(
        "projects/geo-analysis-472713/assets/Provinces"
    )
    region = provinces.geometry()

    datasets = {
        "NDVI": (
            ee.ImageCollection("MODIS/061/MOD13Q1")
            .filterDate(start, end)
            .select("NDVI")
            .mean()
            .multiply(0.0001)
        ),
        "LST": (
            ee.ImageCollection("MODIS/061/MOD11A2")
            .filterDate(start, end)
            .select("LST_Day_1km")
            .mean()
            .multiply(0.02)
            .subtract(273.15)
        ),
        "SOILMOISTURE": (
            ee.ImageCollection("NASA_USDA/HSL/SMAP10KM_soil_moisture")
            .filterDate(start, end)
            .select("ssm")
            .mean()
        ),
        "RAINFALL": (
            ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
            .filterDate(start, end)
            .sum()
        ),
        "FIRECOUNT": (
            ee.ImageCollection("MODIS/061/MOD14A1")
            .filterDate(start, end)
            .select("FireMask")
            .map(lambda img: img.gt(6))
            .sum()
        )
    }

    for var, img in datasets.items():
        asset_id = build_asset_id(var, year, month)
        export_task(img.clip(region), asset_id, region)

    print("✅ All GEE export tasks submitted")


# ----------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ ERROR:", e)
        sys.exit(1)
