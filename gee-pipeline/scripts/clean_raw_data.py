import os
import argparse
import pandas as pd
import numpy as np

# --------------------------------------------------
# Paths
# --------------------------------------------------
RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_DIR = "gee-pipeline/outputs/clean"
os.makedirs(OUTPUT_DIR, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, help="Clean specific variable only (NDVI, LST, Rainfall, SoilMoisture, FireCount)")
args = parser.parse_args()

# --------------------------------------------------
# Cleaning rules
# --------------------------------------------------
LONG_GAP_THRESHOLD = {
    "NDVI": 2,
    "LST": 2,
    "SoilMoisture": 2,
    "Rainfall": 2,
    "FireCount": 2,
}

VALUE_COL = {
    "NDVI": "mean",
    "LST": "mean",
    "SoilMoisture": "mean",
    "Rainfall": "sum",
    "FireCount": "sum",
}

print("🔧 Cleaning raw data…")

# --------------------------------------------------
# Load raw parquet
# --------------------------------------------------
if not os.path.exists(RAW_PARQUET_DIR):
    raise FileNotFoundError(f"❌ RAW_PARQUET_DIR not found: {RAW_PARQUET_DIR}")

files = [f for f in os.listdir(RAW_PARQUET_DIR) if f.endswith(".parquet")]
if not files:
    raise RuntimeError("❌ No parquet files found in raw_parquet")

dfs = [pd.read_parquet(os.path.join(RAW_PARQUET_DIR, f)) for f in files]
df = pd.concat(dfs, ignore_index=True)

df.columns = [c.lower() for c in df.columns]

# Optional variable filter
if args.var:
    args.var = args.var.upper()
    df = df[df["variable"] == args.var]
    if df.empty:
        raise RuntimeError(f"❌ No data found for variable: {args.var}")

# --------------------------------------------------
# Normalize value column
# --------------------------------------------------
def pick_value(row):
    return row.get(VALUE_COL[row["variable"]], np.nan)

df["value"] = df.apply(pick_value, axis=1)

df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))

# --------------------------------------------------
# Required columns (DO NOT TOUCH district/subdistrict)
# --------------------------------------------------
REQUIRED_COLS = [
    "province",
    "district",
    "subdistrict",
    "variable",
    "year",
    "month",
    "value",
]

missing = [c for c in REQUIRED_COLS if c not in df.columns]
if missing:
    raise RuntimeError(f"❌ Missing required columns: {missing}")

# --------------------------------------------------
# NDVI climatology (global monthly)
# --------------------------------------------------
df_ndvi = df[df["variable"] == "NDVI"]
global_ndvi_climatology = (
    df_ndvi.groupby("month")["value"].mean() if not df_ndvi.empty else None
)

# --------------------------------------------------
# Cleaning function
# --------------------------------------------------
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()
    temp = temp.sort_values(
        ["province", "district", "subdistrict", "date"]
    )

    cleaned_groups = []

    for (prov, dist, subdist), g in temp.groupby(
        ["province", "district", "subdistrict"]
    ):
        full_range = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = g.set_index("date").reindex(full_range)

        g[["province", "district", "subdistrict", "variable"]] = (
            g[["province", "district", "subdistrict", "variable"]]
            .ffill()
            .bfill()
        )

        s = pd.to_numeric(g["value"], errors="coerce")

        # find longest missing gap
        is_na = s.isna()
        groups = (is_na != is_na.shift()).cumsum()
        longest_gap = is_na.astype(int).groupby(groups).sum().max()

        # Apply gap logic
        if longest_gap < LONG_GAP_THRESHOLD[var]:
            g["clean_value"] = s.interpolate()

        else:
            if var == "NDVI" and global_ndvi_climatology is not None:
                climat = global_ndvi_climatology.reindex(g.index.month).values
                g["clean_value"] = s.fillna(climat)
            else:
                monthly_mean = s.groupby(g.index.month).transform("mean")
                g["clean_value"] = s.fillna(monthly_mean)

        out = g.reset_index().rename(columns={"index": "date"})
        out["year"] = out["date"].dt.year
        out["month"] = out["date"].dt.month

        # rename clean_value → variable name
        out = out[
            ["province", "district", "subdistrict", "year", "month", "clean_value"]
        ].rename(columns={"clean_value": var})

        cleaned_groups.append(out)

    return pd.concat(cleaned_groups, ignore_index=True)

# --------------------------------------------------
# Run cleaning per variable
# --------------------------------------------------
for var in df["variable"].unique():
    print(f"✨ Cleaning: {var}")
    cleaned_df = clean_variable(df, var)

    out_path = os.path.join(OUTPUT_DIR, f"{var}.parquet")
    cleaned_df.to_parquet(out_path, index=False)

    print(f"✅ Saved: {out_path}")

print("🎉 All variables cleaned successfully!")
