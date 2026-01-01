import pandas as pd
import numpy as np
from scipy.stats import trim_mean
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"
OUTPUT_PATH = "gee-pipeline/outputs/merged/dtw_results.parquet"

VARIABLES = ["NDVI", "RAINFALL", "SOILMOISTURE", "LST", "FIRECOUNT"]
TRIM_RATIO = 0.1

# -----------------------------
# DTW FUNCTIONS
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


# -----------------------------
# LOAD DATA
# -----------------------------
print("Loading dataset...")
df = pd.read_parquet(INPUT_PATH)

df["year_month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)

# -----------------------------
# BASELINE (trimmed mean)
# -----------------------------
print("Computing baseline (trimmed mean)...")
baseline_series = {}

for (district, subdistrict), group in df.groupby(["district", "subdistrict"]):
    baseline_series[(district, subdistrict)] = {}

    for var in VARIABLES:
        monthly_values = []
        for m in range(1, 13):
            vals = group[group["month"] == m][var].dropna().values
            if len(vals) > 0:
                monthly_values.append(trim_mean(vals, TRIM_RATIO))
            else:
                monthly_values.append(np.nan)
        baseline_series[(district, subdistrict)][var] = np.array(monthly_values)


# -----------------------------
# DTW CALCULATION
# -----------------------------
print("Computing DTW distances...")
results = []

for (district, subdistrict), group in df.groupby(["district", "subdistrict"]):
    group = group.sort_values("year_month")

    for year, year_group in group.groupby("year"):
        year_group = year_group.sort_values("month")

        row = {
            "Province": year_group["Province"].iloc[0],
            "District": district,
            "Subdistrict": subdistrict,
            "year": year
        }

        for var in VARIABLES:
            X = year_group[var].values.astype(float)
            Y = baseline_series[(district, subdistrict)][var].astype(float)

            if np.isnan(X).any() or np.isnan(Y).any():
                dist = np.nan
            else:
                dist = dtw_distance(X, Y)

            row[f"DTW_{var}"] = dist

        results.append(row)

dtw_df = pd.DataFrame(results)

# -----------------------------
# THRESHOLDS
# -----------------------------
print("Computing thresholds...")
for var in VARIABLES:
    col = f"DTW_{var}"
    mean = dtw_df[col].mean()
    std = dtw_df[col].std()
    q1 = dtw_df[col].quantile(0.25)
    q3 = dtw_df[col].quantile(0.75)
    iqr = q3 - q1
    p95 = dtw_df[col].quantile(0.95)

    dtw_df[f"{col}_TH_MEAN2STD"] = mean + 2 * std
    dtw_df[f"{col}_TH_IQR"] = q3 + 1.5 * iqr
    dtw_df[f"{col}_TH_P95"] = p95

# -----------------------------
# SAVE OUTPUT
# -----------------------------
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("DTW computation finished.")
print(f"Saved to {OUTPUT_PATH}")
