import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"

OUTPUT_BASE = "gee-pipeline/outputs"
OUTPUT_DTW = f"{OUTPUT_BASE}/merged/dtw_results_normalized.parquet"
OUTPUT_DISTRICT = f"{OUTPUT_BASE}/aggregated/district_anomaly_summary.parquet"
OUTPUT_PROVINCE = f"{OUTPUT_BASE}/aggregated/province_anomaly_summary.parquet"

VARIABLES = ["NDVI", "RAINFALL", "SOILMOISTURE", "LST", "FIRECOUNT"]

ROBUST_THRESHOLD = 3.5
SCORE_CEILING = 5.0

# =====================================================
# FUNCTIONS
# =====================================================
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
                D[i - 1, j - 1],
            )
    return D[N, M]


def calculate_mad(x):
    return median_abs_deviation(x, scale="normal")


# =====================================================
# LOAD DATA
# =====================================================
print("Loading dataset...")
df = pd.read_parquet(INPUT_PATH)
df.columns = df.columns.str.strip().str.lower()

# =====================================================
# 1. COMPUTE ROBUST BASELINE (Median Jan–Dec)
# =====================================================
print("Computing robust baseline (per subdistrict)...")
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

# =====================================================
# 2. COMPUTE DTW PER YEAR (SUBDISTRICT)
# =====================================================
print("Computing DTW per year...")
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

        # store baseline
        for var in VARIABLES:
            baseline_vals = baseline_series[key][var]
            for m in range(12):
                row[f"baseline_{var.lower()}_m{m+1:02d}"] = baseline_vals[m]

        # compute DTW
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

# =====================================================
# 3. MODIFIED Z-SCORE + ROBUST NORMALIZATION
# =====================================================
print("Computing robust anomaly index & flags...")

for var in VARIABLES:
    col = var.lower()
    col_dtw = f"dtw_{col}"

    stats = (
        dtw_df
        .groupby(["district", "subdistrict"])[col_dtw]
        .agg(
            local_median="median",
            local_mad=calculate_mad
        )
        .reset_index()
    )

    dtw_df = dtw_df.merge(stats, on=["district", "subdistrict"], how="left")

    mod_z = f"{col_dtw}_mod_z"
    dtw_df[mod_z] = np.where(
        dtw_df["local_mad"] == 0,
        0,
        0.6745 * (dtw_df[col_dtw] - dtw_df["local_median"]) / dtw_df["local_mad"]
    )

    # Robust score normalization (0–1)
    dtw_df[f"{col_dtw}_index"] = (
        np.clip(np.abs(dtw_df[mod_z]), 0, SCORE_CEILING) / SCORE_CEILING
    )

    # Anomaly flag
    dtw_df[f"{col_dtw}_anomaly_flag"] = (
        np.abs(dtw_df[mod_z]) > ROBUST_THRESHOLD
    ).astype(int)

    dtw_df.drop(columns=["local_median", "local_mad", mod_z], inplace=True)

# =====================================================
# 4. AGGREGATION – DISTRICT LEVEL
# =====================================================
print("Aggregating at DISTRICT level...")

district_frames = []

for var in VARIABLES:
    col = var.lower()

    tmp = (
        dtw_df
        .groupby(["province", "district", "year"])
        .agg(
            anomaly_count=(f"dtw_{col}_anomaly_flag", "sum"),
            anomaly_rate=(f"dtw_{col}_anomaly_flag", "mean"),
            severity_mean=(f"dtw_{col}_index", "mean"),
            severity_max=(f"dtw_{col}_index", "max"),
        )
        .reset_index()
    )

    tmp["variable"] = col
    district_frames.append(tmp)

district_df = pd.concat(district_frames, ignore_index=True)

# =====================================================
# 5. AGGREGATION – PROVINCE LEVEL
# =====================================================
print("Aggregating at PROVINCE level...")

province_frames = []

for var in VARIABLES:
    col = var.lower()

    tmp = (
        dtw_df
        .groupby(["province", "year"])
        .agg(
            anomaly_count=(f"dtw_{col}_anomaly_flag", "sum"),
            anomaly_rate=(f"dtw_{col}_anomaly_flag", "mean"),
            severity_mean=(f"dtw_{col}_index", "mean"),
            severity_max=(f"dtw_{col}_index", "max"),
        )
        .reset_index()
    )

    tmp["variable"] = col
    province_frames.append(tmp)

province_df = pd.concat(province_frames, ignore_index=True)

# =====================================================
# SAVE OUTPUTS
# =====================================================
Path(OUTPUT_DTW).parent.mkdir(parents=True, exist_ok=True)
Path(OUTPUT_DISTRICT).parent.mkdir(parents=True, exist_ok=True)

dtw_df.to_parquet(OUTPUT_DTW, index=False)
district_df.to_parquet(OUTPUT_DISTRICT, index=False)
province_df.to_parquet(OUTPUT_PROVINCE, index=False)

print("------------------------------------------------")
print("PIPELINE FINISHED SUCCESSFULLY")
print(f"Subdistrict DTW   : {OUTPUT_DTW}")
print(f"District summary  : {OUTPUT_DISTRICT}")
print(f"Province summary  : {OUTPUT_PROVINCE}")
print("------------------------------------------------")
