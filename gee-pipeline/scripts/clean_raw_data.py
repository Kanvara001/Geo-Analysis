import os
import argparse
import pandas as pd
import numpy as np
from google.cloud import storage

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
GCS_BUCKET = os.getenv("GCS_BUCKET")        # ต้องตั้งค่าใน GitHub Actions หรือ Local
GCS_PREFIX = "parquet"                     # ตำแหน่งที่เก็บไฟล์ parquet บน GCS

OUTPUT_DIR = "/content/drive/MyDrive/geo_project/clean"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ตัวแปรที่ใช้บ่งชี้คอลัมน์ value
VALUE_COL = {
    "NDVI": "mean",
    "LST": "mean",
    "SoilMoisture": "mean",
    "Rainfall": "sum",
    "FireCount": "sum",
}

# Missing gap logic
LONG_GAP_THRESHOLD = {
    "NDVI": 2,
    "LST": 2,
    "SoilMoisture": 2,
    "Rainfall": 2,
    "FireCount": 2,
}

# -------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------

def apply_physical_filter(df, var):
    """Apply physically reasonable ranges."""
    s = df["value"].copy()

    if var == "NDVI":
        s[(s < -0.2) | (s > 1.0)] = np.nan

    elif var == "LST":
        s[(s < 5) | (s > 55)] = np.nan

    elif var == "SoilMoisture":
        s[(s < 0) | (s > 1)] = np.nan

    elif var == "Rainfall":
        s[s < 0] = np.nan

    elif var == "FireCount":
        s[s < 0] = np.nan

    df["value"] = s
    return df


def remove_iqr_outliers(df):
    """Remove extreme outlier values."""
    s = df["value"].copy()
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr

    df.loc[(s < low) | (s > high), "value"] = np.nan
    return df


def clean_variable(df, var, ndvi_climatology):
    temp = df[df["variable"] == var].copy()
    temp = temp.sort_values(["province", "amphoe", "tambon", "date"])

    cleaned_groups = []

    for (prov, amp, tam), g in temp.groupby(["province", "amphoe", "tambon"]):

        # Full date range
        full = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = g.set_index("date").reindex(full)

        g[["province", "amphoe", "tambon", "variable"]] = (
            g[["province", "amphoe", "tambon", "variable"]].ffill().bfill()
        )

        s = pd.to_numeric(g["value"], errors="coerce")

        # Detect missing gap length
        is_na = s.isna()
        blocks = (is_na != is_na.shift()).cumsum()
        longest_gap = is_na.astype(int).groupby(blocks).sum().max()

        # ---- Clean Logic ----
        if longest_gap < LONG_GAP_THRESHOLD[var]:
            g["clean_value"] = s.interpolate()
        else:
            if var == "NDVI":
                g["clean_value"] = s.fillna(
                    ndvi_climatology.reindex(g.index.month).values
                )
            else:
                monthly_mean = s.groupby(g.index.month).transform("mean")
                g["clean_value"] = s.fillna(monthly_mean)

        cleaned_groups.append(g.reset_index().rename(columns={"index": "date"}))

    return pd.concat(cleaned_groups)


# -------------------------------------------------------
# DOWNLOAD ALL PARQUET FROM GCS
# -------------------------------------------------------

def load_from_gcs():
    client = storage.Client.from_service_account_json(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    bucket = client.bucket(GCS_BUCKET)

    print(f"📥 Loading Parquet from gs://{GCS_BUCKET}/{GCS_PREFIX}")

    dfs = []

    for blob in bucket.list_blobs(prefix=GCS_PREFIX):
        if not blob.name.endswith(".parquet"):
            continue

        print(f"⬇ Download {blob.name}")

        tmp_path = f"/tmp/{os.path.basename(blob.name)}"
        blob.download_to_filename(tmp_path)

        df = pd.read_parquet(tmp_path)
        dfs.append(df)

    if not dfs:
        raise RuntimeError("❌ No parquet files found on GCS")

    return pd.concat(dfs, ignore_index=True)


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, default=None)
args = parser.parse_args()

print("🚀 Cleaning raw data...")

df = load_from_gcs()

df.columns = [c.lower() for c in df.columns]

# Create date column
df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))

# Physical filter + outlier filter
for var in df["variable"].unique():
    df_var = df[df["variable"] == var]
    df.loc[df["variable"] == var] = apply_physical_filter(df_var.copy(), var)
    df.loc[df["variable"] == var] = remove_iqr_outliers(
        df[df["variable"] == var].copy()
    )

# NDVI climatology
ndvi_clim = (
    df[df["variable"] == "NDVI"]
    .groupby("month")["value"]
    .mean()
)

# Clean & save
for var in df["variable"].unique():

    print(f"✨ Cleaning {var}...")

    df_clean = clean_variable(df, var, ndvi_clim)

    out_path = os.path.join(OUTPUT_DIR, f"{var}.parquet")
    df_clean.to_parquet(out_path, index=False)

    print(f"✅ Saved → {out_path}")

print("🎉 ALL DONE!")
