import pandas as pd
import numpy as np
from pathlib import Path

MERGED = Path("gee-pipeline/outputs/merged/merged_dataset.parquet")
OUT = Path("gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet")

KEYS = ["province", "district", "subdistrict"]
TIME = ["year", "month"]
VARS = ["NDVI", "LST", "RAINFALL", "SOILMOISTURE", "FIRECOUNT"]
THRESHOLD = 2

df = pd.read_parquet(MERGED)

# ---------------------------------------
# Build full time grid (สำคัญมาก)
# ---------------------------------------
df["date"] = pd.to_datetime(
    df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
)

full_dates = pd.date_range(df["date"].min(), df["date"].max(), freq="MS")
areas = df[KEYS].drop_duplicates()

grid = pd.DataFrame(
    [
        dict(zip(KEYS, a)) | {"date": d}
        for a in areas.values
        for d in full_dates
    ]
)

df = grid.merge(df, on=KEYS + ["date"], how="left")
df["year"] = df["date"].dt.year
df["month"] = df["date"].dt.month

# ---------------------------------------
# FINAL FILL LOGIC
# ---------------------------------------
for var in VARS:
    if var not in df.columns:
        continue

    print(f"💉 FILL {var}")

    def fill_group(x):
        x = x.sort_values("date")

        # 1) interpolate (time)
        x[var] = x[var].interpolate(limit=THRESHOLD - 1)

        # 2) month climatology (same area)
        miss = x[var].isna()
        if miss.any():
            clim = x.groupby("month")[var].mean()
            x.loc[miss, var] = x.loc[miss, "month"].map(clim)

        return x

    df = df.groupby(KEYS, group_keys=False).apply(fill_group)

    # 3) district mean
    df[var] = df.groupby(["province", "district"])[var].transform(
        lambda x: x.fillna(x.mean())
    )

    # 4) province mean
    df[var] = df.groupby("province")[var].transform(
        lambda x: x.fillna(x.mean())
    )

    # 5) global mean (กันสุดท้าย)
    df[var] = df[var].fillna(df[var].mean())

df.drop(columns="date").to_parquet(OUT, index=False)

print("✅ FINAL FILL COMPLETED")
