import pandas as pd
import numpy as np
from pathlib import Path

RAW_PARQUET_DIR = Path("gee-pipeline/outputs/raw_parquet")
CLEAN_DIR = Path("gee-pipeline/outputs/clean")
THRESHOLD = 2  # months

KEYS = ["province", "district", "subdistric", "year", "month"]

print("🔧 Cleaning raw data...")

# --------------------------------------------------
# Helper functions
# --------------------------------------------------
def apply_iqr(series):
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return series.where(series.between(lower, upper))

def fill_missing_by_gap(df, value_col):
    df = df.sort_values(["year", "month"]).reset_index(drop=True)

    is_na = df[value_col].isna()
    groups = (is_na != is_na.shift()).cumsum()

    for _, g in df.groupby(groups):
        if g[value_col].isna().all():
            if len(g) < THRESHOLD:
                df.loc[g.index, value_col] = df[value_col].interpolate()
            else:
                month = g["month"].iloc[0]
                climatology = df.loc[
                    (df["month"] == month) & df[value_col].notna(),
                    value_col
                ].mean()
                df.loc[g.index, value_col] = climatology
    return df

# --------------------------------------------------
# Load files
# --------------------------------------------------
parquet_files = list(RAW_PARQUET_DIR.rglob("*.parquet"))
if not parquet_files:
    raise RuntimeError("❌ No raw parquet files found")

print(f"📦 Found {len(parquet_files)} files")

for pq in parquet_files:
    variable = pq.parent.name.upper()
    print(f"🧹 Cleaning {variable} → {pq.name}")

    df = pd.read_parquet(pq)

    # --------------------------------------------------
    # Normalize schema
    # --------------------------------------------------
    df = df[[c for c in df.columns if c not in ["id", "variable"]]]

    value_cols = [c for c in df.columns if c not in KEYS]
    if len(value_cols) != 1:
        print(f"⚠️ Skip {pq.name}, ambiguous value columns: {value_cols}")
        continue

    value_col = value_cols[0]
    df = df.rename(columns={value_col: variable})

    # --------------------------------------------------
    # Variable-specific cleaning
    # --------------------------------------------------
    if variable == "LST":
        df[variable] = df[variable] / 1000  # ✅ SCALE
        df.loc[(df[variable] < 5) | (df[variable] > 55), variable] = np.nan

    elif variable == "NDVI":
        df.loc[(df[variable] < -0.2) | (df[variable] > 1.0), variable] = np.nan

    elif variable == "SOILMOISTURE":
        df.loc[(df[variable] < 0) | (df[variable] > 1), variable] = np.nan

    elif variable == "RAINFALL":
        df.loc[df[variable] < 0, variable] = np.nan

    elif variable == "FIRECOUNT":
        df.loc[df[variable] < 0, variable] = np.nan

    # --------------------------------------------------
    # IQR outlier
    # --------------------------------------------------
    df[variable] = apply_iqr(df[variable])

    # --------------------------------------------------
    # Missing gap handling (by area)
    # --------------------------------------------------
    df = (
        df.groupby(["province", "district", "subdistric"], group_keys=False)
        .apply(lambda g: fill_missing_by_gap(g, variable))
    )

    # --------------------------------------------------
    # Save
    # --------------------------------------------------
    out_dir = CLEAN_DIR / variable
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / pq.name, index=False)

print("✅ Cleaning completed successfully")
