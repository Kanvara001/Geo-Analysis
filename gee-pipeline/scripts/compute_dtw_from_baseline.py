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
Z_THRESHOLD = 2.0

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
df.columns = df.columns.str.strip().str.lower()

required_cols = {"province", "district", "subdistrict", "year", "month"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# -----------------------------
# BASELINE (trimmed mean per month per SUBDISTRICT)
# -----------------------------
print("Computing baseline (local subdistrict baseline)...")
baseline_series = {}

for (province, district, subdistrict), group in df.groupby(
    ["province", "district", "subdistrict"]
):
    key = (province, district, subdistrict)
    baseline_series[key] = {}

    for var in VARIABLES:
        col = var.lower()
        monthly_baseline = []

        for m in range(1, 13):
            vals = group[group["month"] == m][col].dropna().values
            if len(vals) > 0:
                monthly_baseline.append(trim_mean(vals, TRIM_RATIO))
            else:
                monthly_baseline.append(np.nan)

        baseline_series[key][var] = np.array(monthly_baseline)

# -----------------------------
# DTW CALCULATION (per year × subdistrict)
# + ADD BASELINE COLUMNS
# -----------------------------
print("Computing DTW distances...")
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
            "year": year
        }

        # ---- baseline columns (12 months) ----
        for var in VARIABLES:
            baseline_vals = baseline_series[key][var]
            for m in range(12):
                row[f"baseline_{var.lower()}_m{m+1:02d}"] = baseline_vals[m]

        # ---- DTW ----
        for var in VARIABLES:
            col = var.lower()
            X = year_group[col].values.astype(float)
            Y = baseline_series[key][var].astype(float)

            if len(X) != 12 or np.isnan(X).any() or np.isnan(Y).any():
                dist = np.nan
            else:
                dist = dtw_distance(X, Y)

            row[f"dtw_{col}"] = dist

        results.append(row)

dtw_df = pd.DataFrame(results)

# -----------------------------
# LOCAL STATS (mean, std per subdistrict)
# -----------------------------
print("Computing local statistics (mean, std)...")

for var in VARIABLES:
    col = f"dtw_{var.lower()}"

    stats = (
        dtw_df
        .groupby(["district", "subdistrict"])[col]
        .agg(["mean", "std"])
        .reset_index()
        .rename(columns={
            "mean": f"{col}_local_mean",
            "std": f"{col}_local_std"
        })
    )

    dtw_df = dtw_df.merge(
        stats,
        on=["district", "subdistrict"],
        how="left"
    )

# -----------------------------
# NORMALIZATION (Z-score per subdistrict)
# -----------------------------
print("Normalizing DTW (Z-score per subdistrict)...")

for var in VARIABLES:
    col = f"dtw_{var.lower()}"

    dtw_df[f"{col}_z"] = (
        dtw_df[col] - dtw_df[f"{col}_local_mean"]
    ) / dtw_df[f"{col}_local_std"]

# -----------------------------
# LOCAL THRESHOLD (mean + 2σ)
# -----------------------------
print("Computing local thresholds (mean + 2σ)...")

for var in VARIABLES:
    col = f"dtw_{var.lower()}"

    dtw_df[f"{col}_local_threshold"] = (
        dtw_df[f"{col}_local_mean"] +
        2 * dtw_df[f"{col}_local_std"]
    )

    dtw_df[f"{col}_flag"] = (
        dtw_df[col] > dtw_df[f"{col}_local_threshold"]
    ).astype(int)

# -----------------------------
# GLOBAL THRESHOLD (Z-score)
# -----------------------------
print("Applying global Z-score threshold...")

for var in VARIABLES:
    col = f"dtw_{var.lower()}"

    dtw_df[f"{col}_z_flag"] = (
        dtw_df[f"{col}_z"] > Z_THRESHOLD
    ).astype(int)

# -----------------------------
# SAVE OUTPUT
# -----------------------------
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("DTW computation finished.")
print(f"Saved to {OUTPUT_PATH}")
