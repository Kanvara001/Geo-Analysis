import os
import argparse
import pandas as pd
import numpy as np

RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_DIR = "gee-pipeline/outputs/clean"

os.makedirs(OUTPUT_DIR, exist_ok=True)

parser = argparse.ArgumentParser()
parser.add_argument("--var", type=str, help="Clean a specific variable only")
args = parser.parse_args()

# ----------- Cleaning Rules -----------

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

if not os.path.exists(RAW_PARQUET_DIR):
    raise FileNotFoundError(f"❌ RAW_PARQUET_DIR does not exist: {RAW_PARQUET_DIR}")

files = [f for f in os.listdir(RAW_PARQUET_DIR) if f.endswith(".parquet")]

if len(files) == 0:
    raise RuntimeError("❌ No parquet files found in raw_parquet folder")

dfs = [pd.read_parquet(os.path.join(RAW_PARQUET_DIR, f)) for f in files]
df = pd.concat(dfs, ignore_index=True)

df.columns = [c.lower() for c in df.columns]

if args.var:
    df = df[df["variable"] == args.var.upper()]
    if df.empty:
        raise RuntimeError(f"❌ No data found for variable '{args.var}'")


def pick_value(row):
    col = VALUE_COL[row["variable"]]
    return row.get(col, np.nan)


df["value"] = df.apply(pick_value, axis=1)
df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))

required_cols = ["province", "amphoe", "tambon", "variable", "year", "month", "value"]
missing = [c for c in required_cols if c not in df.columns]

if missing:
    raise RuntimeError(f"❌ Missing required columns: {missing}")


# NDVI climatology
df_ndvi = df[df["variable"] == "NDVI"].copy()
global_climatology_NDVI = (
    df_ndvi.groupby("month")["value"].mean() if not df_ndvi.empty else None
)


def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()
    temp = temp.sort_values(["province", "amphoe", "tambon", "date"])

    results = []

    for (prov, amp, tam), g in temp.groupby(["province", "amphoe", "tambon"]):

        full_range = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = g.set_index("date").reindex(full_range)

        g[["province", "amphoe", "tambon", "variable"]] = \
            g[["province", "amphoe", "tambon", "variable"]].ffill().bfill()

        s = pd.to_numeric(g["value"], errors="coerce")

        is_na = s.isna()
        groups = (is_na != is_na.shift()).cumsum()
        longest_gap = is_na.astype(int).groupby(groups).sum().max()

        if longest_gap < LONG_GAP_THRESHOLD[var]:
            g["clean_value"] = s.interpolate()

        else:
            if var == "NDVI" and global_climatology_NDVI is not None:
                monthly = global_climatology_NDVI.reindex(g.index.month).values
                g["clean_value"] = s.fillna(monthly)
            else:
                monthly_mean = s.groupby(g.index.month).transform("mean")
                g["clean_value"] = s.fillna(monthly_mean)

        results.append(g.reset_index().rename(columns={"index": "date"}))

    return pd.concat(results)


for var in df["variable"].unique():
    print(f"✨ Cleaning: {var}")
    cleaned = clean_variable(df, var)

    out_path = os.path.join(OUTPUT_DIR, f"{var}.parquet")
    cleaned.to_parquet(out_path, index=False)

    print(f"✅ Cleaned {var} → {out_path}")

print("🎉 All variables cleaned successfully!")
