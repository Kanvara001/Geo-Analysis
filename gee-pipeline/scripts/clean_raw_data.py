import pandas as pd
import numpy as np
import os
import glob

RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CLEAN = "gee-pipeline/outputs/clean"

os.makedirs(OUTPUT_CLEAN, exist_ok=True)

# Threshold (เดือน) สำหรับ defining "long gap"
LONG_GAP_THRESHOLD = {
    "NDVI": 2,
    "LST": 2,
    "SoilMoisture": 2,
    "Rainfall": 2,
    "FireCount": 2,
}

# โหลดทุกไฟล์ parquet และรวมเป็น DataFrame เดียว
def load_all():
    files = glob.glob(f"{RAW_PARQUET_DIR}/*.parquet")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

df = load_all()

# สร้าง column date สำหรับ time-series
df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))

# ฟังก์ชัน clean variable
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()
    temp["value"] = pd.to_numeric(temp["value"], errors="coerce")

    # Sort สำหรับ interpolation
    temp = temp.sort_values(["province", "amphoe", "tambon", "date"])

    cleaned_groups = []

    # Group ตามพื้นที่
    for (prov, amph, tambon), g in temp.groupby(["province", "amphoe", "tambon"]):
        # Reindex ให้ครบทุกเดือน
        full_idx = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = g.set_index("date").reindex(full_idx)
        g[["province","amphoe","tambon","variable","year","month"]] = g[["province","amphoe","tambon","variable","year","month"]].ffill()
        g["year"] = g.index.year
        g["month"] = g.index.month

        s = g["value"]

        # คำนวณ longest consecutive missing gap
        is_na = s.isna()
        groups = (is_na != is_na.shift()).cumsum()
        longest_gap = is_na.groupby(groups).sum().max()

        if longest_gap < LONG_GAP_THRESHOLD[var]:
            # Short gap → interpolate
            g["clean_value"] = s.interpolate()
        else:
            # Long gap → เติมตามกฎ variable
            if var == "NDVI":
                # Monthly climatology (mean across all years)
                climatology = s.groupby(s.index.month).transform("mean")
                g["clean_value"] = s.fillna(climatology)
            else:
                # Monthly mean per group
                monthly_mean = s.groupby(s.index.month).transform("mean")
                g["clean_value"] = s.fillna(monthly_mean)

        cleaned_groups.append(g.reset_index())

    return pd.concat(cleaned_groups)

# ทำความสะอาดทุกตัวแปรและบันทึกแยกไฟล์
for var in df["variable"].unique():
    clean_df = clean_variable(df, var)
    out_file = os.path.join(OUTPUT_CLEAN, f"{var}.parquet")
    clean_df.to_parquet(out_file, index=False)
    print(f"✅ Cleaned {var} → {out_file}")

print("🎉 Cleaning complete for all variables!")
