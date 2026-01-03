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

# normalize column names
df.columns = df.columns.str.strip().str.lower()

required_cols = {"province", "district", "subdistrict", "year", "month"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# -----------------------------
# BASELINE (trimmed mean per month per subdistrict)
# -----------------------------
print("Computing baseline (trimmed mean)...")
baseline_series = {}

for (district, subdistrict), group in df.groupby(["district", "subdistrict"]):
    baseline_series[(district, subdistrict)] = {}

    for var in VARIABLES:
        col = var.lower()
        monthly_baseline = []

        for m in range(1, 13):
            vals = group[group["month"] == m][col].dropna().values
            if len(vals) > 0:
                monthly_baseline.append(trim_mean(vals, TRIM_RATIO))
            else:
                monthly_baseline.append(np.nan)

        baseline_series[(district, subdistrict)][var] = np.array(monthly_baseline)

# -----------------------------
# DTW CALCULATION (per year)
# -----------------------------
print("Computing DTW distances...")
results = []

for (district, subdistrict), group in df.groupby(["district", "subdistrict"]):

    for year, year_group in group.groupby("year"):
        year_group = year_group.sort_values("month")

        row = {
            "province": year_group["province"].iloc[0],
            "district": district,
            "subdistrict": subdistrict,
            "year": year
        }

        for var in VARIABLES:
            col = var.lower()
            X = year_group[col].values.astype(float)
            Y = baseline_series[(district, subdistrict)][var].astype(float)

            if len(X) != 12 or np.isnan(X).any() or np.isnan(Y).any():
                dist = np.nan
            else:
                dist = dtw_distance(X, Y)

            row[f"dtw_{col}"] = dist

        results.append(row)

dtw_df = pd.DataFrame(results)

# -----------------------------
# THRESHOLD (GLOBAL: all years × all areas)
# -----------------------------
print("Computing global thresholds (mean + 2σ)...")

for var in VARIABLES:
    col = f"dtw_{var.lower()}"

    mean = dtw_df[col].mean()
    std = dtw_df[col].std()
    threshold = mean + 2 * std

    dtw_df[f"{col}_threshold"] = threshold
    dtw_df[f"{col}_flag"] = (dtw_df[col] > threshold).astype(int)

# -----------------------------
# SAVE OUTPUT
# -----------------------------
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("DTW computation finished.")
print(f"Saved to {OUTPUT_PATH}")
