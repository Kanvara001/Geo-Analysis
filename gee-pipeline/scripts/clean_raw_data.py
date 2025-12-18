import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path("gee-pipeline/outputs/raw_parquet")
CLEAN_DIR = Path("gee-pipeline/outputs/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["province", "district", "subdistrict", "year", "month"]
THRESHOLD = 2  # months

# --------------------------------------------------
# Utility functions
# --------------------------------------------------
def iqr_filter(series):
    q1, q3 = series.quantile([0.25, 0.75])
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    return series.where(series.between(lo, hi))

def fill_missing_monthly(df, col):
    df = df.sort_values(["year", "month"])
    s = df[col]

    # interpolate small gaps
    s_interp = s.interpolate(limit=THRESHOLD-1)

    # long gaps → monthly climatology
    clim = df.groupby("month")[col].mean()
    for i in range(len(s_interp)):
        if pd.isna(s_interp.iloc[i]):
            s_interp.iloc[i] = clim.loc[df.iloc[i]["month"]]

    return s_interp

# --------------------------------------------------
# Loop raw parquet
# --------------------------------------------------
for pq in RAW_DIR.rglob("*.parquet"):
    VAR = pq.parent.name.upper()
    print(f"🧹 CLEAN {VAR} → {pq.name}")

    df = pd.read_parquet(pq)

    # ---------- schema normalize ----------
    if "subdistric" in df.columns:
        df = df.rename(columns={"subdistric": "subdistrict"})

    # drop garbage columns
    df = df.drop(columns=[c for c in df.columns if c not in KEYS + ["mean", "sum"]])

    value_col = "mean" if "mean" in df.columns else "sum"
    df = df.rename(columns={value_col: VAR})

    # ---------- variable specific cleaning ----------
    if VAR == "LST":
        # scale MODIS
        df[VAR] = df[VAR] * 0.02 - 273.15
        df[VAR] = df[VAR].where(df[VAR].between(5, 55))
        df[VAR] = iqr_filter(df[VAR])

    elif VAR == "NDVI":
        df[VAR] = df[VAR].where(df[VAR].between(-0.2, 1.0))
        df[VAR] = df[VAR].replace(0, np.nan)
        df[VAR] = iqr_filter(df[VAR])

    elif VAR == "SOILMOISTURE":
        df[VAR] = df[VAR].where(df[VAR].between(0, 1))
        df[VAR] = iqr_filter(df[VAR])

    elif VAR == "RAINFALL":
        df[VAR] = df[VAR].where(df[VAR] >= 0)
        df[VAR] = iqr_filter(df[VAR])

    elif VAR == "FIRECOUNT":
        df[VAR] = df[VAR].where(df[VAR] >= 0)
        df[VAR] = iqr_filter(df[VAR])

    # ---------- missing handling ----------
    df[VAR] = fill_missing_monthly(df, VAR)

    # ---------- save ----------
    out_dir = CLEAN_DIR / VAR
    out_dir.mkdir(exist_ok=True)
    df[KEYS + [VAR]].to_parquet(out_dir / pq.name, index=False)

print("✅ CLEAN COMPLETED — schema fixed, scale fixed, safe to merge")
