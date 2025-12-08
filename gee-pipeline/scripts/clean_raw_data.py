import os
import argparse
import pandas as pd
import numpy as np
import glob

# -----------------------------
# Corrected RAW parquet path
# -----------------------------
RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CLEAN = "gee-pipeline/outputs/clean"

os.makedirs(OUTPUT_CLEAN, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, help="Clean a specific variable only")
args = parser.parse_args()


# ---------------------------------------------------------
# VALID VARIABLES (exactly match what GeoJSON produces)
# ---------------------------------------------------------
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
    "Rainfall": "sum",       # CHIRPS daily aggregated monthly → sum
    "FireCount": "sum",      # fire pixels counted
}

# ---------------------------------------------------------
# Load all Parquet files
# ---------------------------------------------------------
print("🔧 Cleaning raw data…")

if not os.path.exists(RAW_PARQUET_DIR):
    raise FileNotFoundError(f"❌ RAW_PARQUET_DIR does not exist: {RAW_PARQUET_DIR}")

files = [f for f in os.listdir(RAW_PARQUET_DIR) if f.endswith(".parquet")]

if len(files) == 0:
    raise RuntimeError("❌ No parquet files found in raw_parquet folder")

dfs = [pd.read_parquet(os.path.join(RAW_PARQUET_DIR, f)) for f in files]
df = pd.concat(dfs, ignore_index=True)

# Normalize column names
df.columns = [c.lower() for c in df.columns]

# Optional filter
if args.var:
    df = df[df["variable"] == args.var]
    if df.empty:
        raise RuntimeError(f"❌ No data found for variable '{args.var}'")


# ---------------------------------------------------------
# Normalize "value" column
# ---------------------------------------------------------
def pick_value(row):
    col = VALUE_COL[row["variable"]]
    return row.get(col, np.nan)

df["value"] = df.apply(pick_value, axis=1)

# Create date column
df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))


# ---------------------------------------------------------
# Make sure column exist (geometry removed earlier)
# ---------------------------------------------------------
REQUIRED = ["province", "amphoe", "tambon", "variable", "year", "month", "value"]

missing = [c for c in REQUIRED if c not in df.columns]
if missing:
    raise RuntimeError(f"❌ Missing required columns: {missing}")


# ---------------------------------------------------------
# NDVI global climatology
# ---------------------------------------------------------
df_ndvi = df[df["variable"] == "NDVI"].copy()
if not df_ndvi.empty:
    global_climatology_NDVI = df_ndvi.groupby("month")["value"].mean()
else:
    global_climatology_NDVI = None


# ---------------------------------------------------------
# Clean per variable
# ---------------------------------------------------------
def clean_variable(df, var):

    temp = df[df["variable"] == var].copy()
    temp = temp.sort_values(["province", "amphoe", "tambon", "date"])

    groups_cleaned = []

    for (prov, amp, tam), g in temp.groupby(["province", "amphoe", "tambon"]):

        full_range = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = g.set_index("date").reindex(full_range)

        # Fill static metadata
        g[["province", "amphoe", "tambon", "variable"]] = (
            g[["province", "amphoe", "tambon", "variable"]].ffill().bfill()
        )

        s = pd.to_numeric(g["value"], errors="coerce")

        # Detect longest NA gap
        is_na = s.isna()
        groups = (is_na != is_na.shift()).cumsum()
        longest_gap = is_na.astype(int).groupby(groups).sum().max()

        # Apply cleaning rules
        if longest_gap < LONG_GAP_THRESHOLD[var]:
            g["clean_value"] = s.interpolate()

        else:
            if var == "NDVI" and global_climatology_NDVI is not None:
                climat = global_climatology_NDVI.reindex(g.index.month).values
                g["clean_value"] = s.fillna(climat)
            else:
                monthly_mean = s.groupby(g.index.month).transform("mean")
                g["clean_value"] = s.fillna(monthly_mean)

        groups_cleaned.append(g.reset_index().rename(columns={"index": "date"}))

    return pd.concat(groups_cleaned)


# ---------------------------------------------------------
# Loop through variables and save cleaned parquet
# ---------------------------------------------------------
for var in df["variable"].unique():
    print(f"✨ Cleaning: {var}")
    clean_df = clean_variable(df, var)

    out_path = os.path.join(OUTPUT_CLEAN, f"{var}.parquet")
    clean_df.to_parquet(out_path, index=False)

    print(f"✅ Cleaned {var} → {out_path}")

print("🎉 All variables cleaned successfully!")
