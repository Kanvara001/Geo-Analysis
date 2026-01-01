import pandas as pd
import numpy as np
from scipy.stats import trim_mean
from tslearn.metrics import dtw

# -----------------------------
# CONFIG
# -----------------------------
PARQUET_PATH = "merged_dataset_FILLED.parquet"
OUTPUT_PATH = "dtw_distance.parquet"

VALUE_COLS = [
    "NDVI",
    "Rainfall",
    "SoilMoisture",
    "LST",
    "FireCount"
]

TRIM_RATIO = 0.1   # 10% trim mean

# -----------------------------
# LOAD DATA
# -----------------------------
df = pd.read_parquet(PARQUET_PATH)

df = df.sort_values(["area_id", "year", "month"]).reset_index(drop=True)

# -----------------------------
# 1) CREATE BASELINE (Trimmed Mean)
# baseline = (area_id, month, variable)
# -----------------------------
baseline = (
    df
    .groupby(["area_id", "month"])[VALUE_COLS]
    .agg(lambda x: trim_mean(x.dropna(), TRIM_RATIO))
    .reset_index()
)

baseline = baseline.rename(
    columns={col: f"{col}_baseline" for col in VALUE_COLS}
)

# -----------------------------
# 2) MERGE BASELINE BACK
# -----------------------------
df = df.merge(
    baseline,
    on=["area_id", "month"],
    how="left"
)

# -----------------------------
# 3) DTW CALCULATION
# DTW per (area_id, year, month)
# -----------------------------
results = []

for (area_id, year), g in df.groupby(["area_id", "year"]):

    g = g.sort_values("month")

    for var in VALUE_COLS:
        series_actual = g[var].values.astype(float)
        series_baseline = g[f"{var}_baseline"].values.astype(float)

        # ต้องไม่มี NaN
        if np.isnan(series_actual).any() or np.isnan(series_baseline).any():
            continue

        distance = dtw(series_actual, series_baseline)

        results.append({
            "area_id": area_id,
            "year": year,
            "variable": var,
            "dtw_distance": distance
        })

# -----------------------------
# SAVE OUTPUT
# -----------------------------
dtw_df = pd.DataFrame(results)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("✅ DTW computation completed")
print(dtw_df.head())
