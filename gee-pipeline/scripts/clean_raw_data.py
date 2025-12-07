import argparse
import pandas as pd
import numpy as np
import os
import glob

RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CLEAN = "gee-pipeline/outputs/clean"
os.makedirs(OUTPUT_CLEAN, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, help="Clean a specific variable only")
args = parser.parse_args()

# ---------------------------------------------------------------------
#   VALID VARIABLES FROM GEOJSON
#   Rainfall, FireCount, LST, NDVI, SoilMoisture
# ---------------------------------------------------------------------
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
    "Rainfall": "mean",
    "SoilMoisture": "mean",
    "FireCount": "sum",
}

# ---------------- Load Parquet -------------------
files = glob.glob(f"{RAW_PARQUET_DIR}/*.parquet")
dfs = [pd.read_parquet(f) for f in files]

if len(dfs) == 0:
    raise RuntimeError("❌ No parquet files found")

df = pd.concat(dfs, ignore_index=True)

# Optional: clean only a selected variable
if args.var:
    df = df[df["variable"] == args.var]
    if df.empty:
        raise RuntimeError(f"❌ No data found for variable {args.var}")

# Normalize value column
df["value"] = df.apply(lambda r: r[VALUE_COL[r["variable"]]], axis=1)

# Create date column
df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))

# NDVI global climatology
df_ndvi = df[df["variable"] == "NDVI"].copy()
global_climatology_NDVI = df_ndvi.groupby("month")["value"].mean()

# ---------------- Clean function -------------------
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()
    temp = temp.sort_values(["province", "amphoe", "tambon", "date"])
    groups_cleaned = []

    for (prov, amp, tam), g in temp.groupby(["province", "amphoe", "tambon"]):
        full_range = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = g.set_index("date").reindex(full_range)

        # Fill static columns
        g[["province", "amphoe", "tambon", "variable"]] = (
            g[["province", "amphoe", "tambon", "variable"]].ffill().bfill()
        )

        s = pd.to_numeric(g["value"], errors="coerce")

        # Detect longest NA gap
        is_na = s.isna()
        groups = (is_na != is_na.shift()).cumsum()
        longest_gap = is_na.astype(int).groupby(groups).sum().max()

        # === Cleaning rules ===
        if longest_gap < LONG_GAP_THRESHOLD[var]:
            g["clean_value"] = s.interpolate()
        else:
            if var == "NDVI":  
                climat = global_climatology_NDVI.reindex(g.index.month).values
                g["clean_value"] = s.fillna(climat)
            else:
                monthly_mean = s.groupby(g.index.month).transform("mean")
                g["clean_value"] = s.fillna(monthly_mean)

        groups_cleaned.append(
            g.reset_index().rename(columns={"index": "date"})
        )

    return pd.concat(groups_cleaned)


# --------------- Run cleaning per variable ------------------
for var in df["variable"].unique():
    clean_df = clean_variable(df, var)
    out_path = os.path.join(OUTPUT_CLEAN, f"{var}.parquet")
    clean_df.to_parquet(out_path, index=False)
    print(f"✅ Cleaned {var} → {out_path}")

print("🎉 Clean OK")
