import pandas as pd
import numpy as np
import os
import glob
import sys

RAW_PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CLEAN = "gee-pipeline/outputs/clean"

os.makedirs(OUTPUT_CLEAN, exist_ok=True)

LONG_GAP_THRESHOLD = {
    "NDVI": 2,
    "LST": 2,
    "SoilMoisture": 2,
    "Rainfall": 2,       # rainfall variable name
    "FireCount": 2,
}

# ------------- Load all parquet -------------
def load_all():
    files = glob.glob(f"{RAW_PARQUET_DIR}/*.parquet")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

df = load_all()

# Fix rainfall variable name if needed
df["variable"] = df["variable"].replace({
    "rainfall": "Rainfall",
    "precipitation": "Rainfall"
})

df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))

global_climatology_NDVI = (
    df[df["variable"] == "NDVI"]
    .groupby(df["month"])["value"]
    .mean()
)

# ------------- Clean per variable -------------
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()
    temp["value"] = pd.to_numeric(temp["value"], errors="coerce")
    temp = temp.sort_values(["province", "amphoe", "tambon", "date"])

    cleaned_groups = []

    for (prov, amph, tambon), g in temp.groupby(["province", "amphoe", "tambon"]):

        full_idx = pd.date_range(g["date"].min(), g["date"].max(), freq="MS")
        g = g.set_index("date").reindex(full_idx)

        cols = ["province","amphoe","tambon","variable","year","month"]
        g[cols] = g[cols].ffill().bfill()

        g["year"] = g.index.year
        g["month"] = g.index.month

        s = g["value"]

        is_na = s.isna()
        groups = (is_na != is_na.shift()).cumsum()
        longest_gap = is_na.astype(int).groupby(groups).sum().max()

        if longest_gap < LONG_GAP_THRESHOLD[var]:
            g["clean_value"] = s.interpolate()
        else:
            if var == "NDVI":
                climat = global_climatology_NDVI.reindex(g["month"]).values
                g["clean_value"] = s.fillna(climat)
            else:
                monthly_mean = s.groupby(g.index.month).transform("mean")
                g["clean_value"] = s.fillna(monthly_mean)

        cleaned_groups.append(
            g.reset_index().rename(columns={"index": "date"})
        )

    return pd.concat(cleaned_groups)

# ------------- Run cleaning -------------
all_vars = df["variable"].unique()

# Optional: Run only selected variable
if len(sys.argv) > 1:
    selected_var = sys.argv[1]
    print(f"⚡ Running ONLY variable: {selected_var}")
    all_vars = [selected_var]

for var in all_vars:
    clean_df = clean_variable(df, var)
    out_file = os.path.join(OUTPUT_CLEAN, f"{var}.parquet")
    clean_df.to_parquet(out_file, index=False)
    print(f"✅ Cleaned {var} → {out_file}")

print("🎉 Cleaning complete!")
