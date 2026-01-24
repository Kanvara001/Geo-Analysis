import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"
OUTPUT_PATH = "gee-pipeline/outputs/merged/dtw_results_normalized.parquet"

VARIABLES = ["NDVI", "RAINFALL", "SOILMOISTURE", "LST", "FIRECOUNT"]

ROBUST_THRESHOLD = 3.5      # Threshold สำหรับ Modified Z-score
SCORE_CEILING = 5.0         # เพดานสำหรับทำ index 0–1

# -----------------------------
# FUNCTIONS
# -----------------------------
def compute_cost_matrix(X, Y):
    N, M = len(X), len(Y)
    C = np.zeros((N, M))
    for i in range(N):
        for j in range(M):
            C[i, j] = abs(X[i] - Y[j])
    return C

def dtw_distance(X, Y):
    C = compute_cost_matrix(X, Y)
    N, M = C.shape
    D = np.full((N + 1, M + 1), np.inf)
    D[0, 0] = 0
    for i in range(1, N + 1):
        for j in range(1, M + 1):
            D[i, j] = C[i - 1, j - 1] + min(
                D[i - 1, j],
                D[i, j - 1],
                D[i - 1, j - 1]
            )
    return D[N, M]

def calculate_mad(x):
    return median_abs_deviation(x, scale="normal")

# -----------------------------
# LOAD DATA
# -----------------------------
print("Loading dataset...")
df = pd.read_parquet(INPUT_PATH)
df.columns = df.columns.str.strip().str.lower()

# -----------------------------
# 1. COMPUTE ROBUST BASELINE (Median Jan–Dec)
# -----------------------------
print("Computing robust baseline (Median per Subdistrict)...")
baseline_series = {}

for (province, district, subdistrict), group in df.groupby(
    ["province", "district", "subdistrict"]
):
    key = (province, district, subdistrict)
    baseline_series[key] = {}

    for var in VARIABLES:
        col = var.lower()
        monthly_vals = [
            group[group["month"] == m][col].dropna().values
            for m in range(1, 13)
        ]

        baseline_series[key][var] = np.array(
            [np.median(v) if len(v) > 0 else np.nan for v in monthly_vals]
        )

# -----------------------------
# 2. COMPUTE DTW PER YEAR
# -----------------------------
print("Computing DTW distances & storing baselines...")
results = []

for (province, district, subdistrict), group in df.groupby(
    ["province", "district", "subdistrict"]
):
    key = (province, district, subdistrict)

    for year, year_group in group.groupby("year"):
        year_group = year_group.sort_values("month")

        row = {
            "province": province,
            "district": district,
            "subdistrict": subdistrict,
            "year": year,
        }

        # Store baseline (12 months × variables)
        for var in VARIABLES:
            baseline_vals = baseline_series[key][var]
            for m in range(12):
                row[f"baseline_{var.lower()}_m{m+1:02d}"] = baseline_vals[m]

        # Compute DTW
        for var in VARIABLES:
            col = var.lower()
            X = year_group[col].values.astype(float)
            Y = baseline_series[key][var].astype(float)

            if len(X) != 12 or np.isnan(X).any() or np.isnan(Y).any():
                row[f"dtw_{col}"] = np.nan
            else:
                row[f"dtw_{col}"] = dtw_distance(X, Y)

        results.append(row)

dtw_df = pd.DataFrame(results)

# -----------------------------
# 3. MODIFIED Z-SCORE + ROBUST NORMALIZATION
# -----------------------------
print("Computing anomaly flags & indices (robust)...")

for var in VARIABLES:
    col_dtw = f"dtw_{var.lower()}"

    # 3.1 Robust stats per area
    stats = dtw_df.groupby(
        ["district", "subdistrict"]
    )[col_dtw].agg(
        local_median="median",
        local_mad=calculate_mad
    ).reset_index()

    dtw_df = dtw_df.merge(stats, on=["district", "subdistrict"], how="left")

    # 3.2 Modified Z-score
    mod_z_col = f"{col_dtw}_mod_z"
    dtw_df[mod_z_col] = np.where(
        dtw_df["local_mad"] == 0,
        0,
        0.6745 * (dtw_df[col_dtw] - dtw_df["local_median"]) / dtw_df["local_mad"]
    )

    # 3.3 Robust score normalization (0–1)
    index_col = f"{col_dtw}_index"
    dtw_df[index_col] = (
        np.clip(np.abs(dtw_df[mod_z_col]), 0, SCORE_CEILING) / SCORE_CEILING
    )

    # 3.4 Anomaly flag (retain all data)
    dtw_df[f"{col_dtw}_anomaly_flag"] = (
        np.abs(dtw_df[mod_z_col]) > ROBUST_THRESHOLD
    ).astype(int)

    # Clean up
    dtw_df.drop(
        columns=["local_median", "local_mad", mod_z_col],
        inplace=True
    )

# -----------------------------
# SAVE OUTPUT
# -----------------------------
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("------------------------------------------------")
print(f"Saved to: {OUTPUT_PATH}")
print("Example columns:")
print(" - dtw_ndvi_index (0–1 anomaly magnitude)")
print(" - dtw_ndvi_anomaly_flag (0/1)")
print(" - baseline_ndvi_m01 ... baseline_ndvi_m12")
print("------------------------------------------------")
