import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path("gee-pipeline/outputs/raw_parquet")
CLEAN_DIR = Path("gee-pipeline/outputs/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["province", "district", "subdistrict", "year", "month"]
THRESHOLD = 2  # months

# -------------------------------
def iqr_filter(s):
    q1, q3 = s.quantile([0.25, 0.75])
    iqr = q3 - q1
    return s.where((s >= q1 - 1.5 * iqr) & (s <= q3 + 1.5 * iqr))

def fill_missing(df, col):
    df = df.sort_values(["year", "month"])

    # interpolate gaps < threshold
    df[col] = df[col].interpolate(limit=THRESHOLD - 1)

    # remaining → monthly climatology
    missing = df[col].isna()
    if missing.any():
        clim = df.groupby("month")[col].mean()
        df.loc[missing, col] = df.loc[missing, "month"].map(clim)

    return df

# -------------------------------
VALUE_COLUMN_MAP = {
    "LST": "mean",
    "NDVI": "mean",
    "SOILMOISTURE": "mean",
    "RAINFALL": "sum",
    "FIRECOUNT": "sum",
}

# -------------------------------
for pq in RAW_DIR.rglob("*.parquet"):
    var = pq.parent.name.upper()
    print(f"🧹 Cleaning {var}: {pq.name}")

    if var not in VALUE_COLUMN_MAP:
        print(f"⚠️ Unknown variable: {var}")
        continue

    df = pd.read_parquet(pq)

    # normalize column names
    df = df.rename(columns={"subdistric": "subdistrict"})

    value_col = VALUE_COLUMN_MAP[var]
    if value_col not in df.columns:
        raise RuntimeError(f"{pq.name} missing '{value_col}' column")

    df = df[KEYS + [value_col]].rename(columns={value_col: var})

    # ================= VARIABLE RULES =================
    if var == "LST":
        # MODIS LST scale factor
        df[var] = df[var] * 0.02 - 273.15
        df[var] = df[var].where(df[var].between(5, 55))
        df[var] = iqr_filter(df[var])

    elif var == "NDVI":
        df[var] = df[var] / 10000
        df[var] = df[var].where(df[var].between(-0.2, 1.0))
        df.loc[df[var] == 0, var] = np.nan
        df[var] = iqr_filter(df[var])

    elif var == "SOILMOISTURE":
        df[var] = df[var].where(df[var].between(0, 1))
        df[var] = iqr_filter(df[var])

    elif var == "RAINFALL":
        # ❗ ไม่ตัด outlier ตามที่คุณขอ
        df[var] = df[var].where(df[var] >= 0)

    elif var == "FIRECOUNT":
        df[var] = df[var].where(df[var] >= 0)
        df[var] = iqr_filter(df[var])

    # ================= MISSING LOGIC =================
    df = df.groupby(KEYS[:-2], group_keys=False).apply(
        lambda x: fill_missing(x, var)
    )

    out_dir = CLEAN_DIR / var
    out_dir.mkdir(exist_ok=True)
    df.to_parquet(out_dir / pq.name, index=False)

print("✅ CLEAN COMPLETED — READY FOR MERGE")
