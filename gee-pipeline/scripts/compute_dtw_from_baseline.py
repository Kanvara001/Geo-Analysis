import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"

OUTPUT_DISTRICT = "gee-pipeline/outputs/aggregated/district_dtw_results.parquet"
OUTPUT_PROVINCE = "gee-pipeline/outputs/aggregated/province_dtw_results.parquet"

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
# FUNCTION: RUN PIPELINE FOR ANY LEVEL
# =====================================================
def run_dtw_pipeline(df, group_cols, output_path):
    """
    group_cols:
      - ["province", "district"]  -> district level
      - ["province"]              -> province level
    """

    # -------------------------------------------------
    # 1. AGG TO MONTHLY (MEAN)
    # -------------------------------------------------
    print(f"Agg monthly data for {group_cols}...")

    agg_dict = {v.lower(): "mean" for v in VARIABLES}

    monthly = (
        df
        .groupby(group_cols + ["year", "month"], as_index=False)
        .agg(agg_dict)
    )

    # -------------------------------------------------
    # 2. COMPUTE ROBUST BASELINE (Median Jan–Dec)
    # -------------------------------------------------
    print("Computing robust baseline...")
    baseline_series = {}

    for keys, group in monthly.groupby(group_cols):
        baseline_series[keys] = {}

        for var in VARIABLES:
            col = var.lower()
            monthly_vals = [
                group[group["month"] == m][col].dropna().values
                for m in range(1, 13)
            ]

            baseline_series[keys][var] = np.array(
                [np.median(v) if len(v) > 0 else np.nan for v in monthly_vals]
            )

    # -------------------------------------------------
    # 3. COMPUTE DTW PER YEAR
    # -------------------------------------------------
    print("Computing DTW...")
    rows = []

    for keys, group in monthly.groupby(group_cols):
        for year, year_group in group.groupby("year"):
            year_group = year_group.sort_values("month")

            row = {col: val for col, val in zip(group_cols, keys if isinstance(keys, tuple) else [keys])}
            row["year"] = year

            # store baseline
            for var in VARIABLES:
                base = baseline_series[keys][var]
                for m in range(12):
                    row[f"baseline_{var.lower()}_m{m+1:02d}"] = base[m]

            # DTW
            for var in VARIABLES:
                col = var.lower()
                X = year_group[col].values.astype(float)
                Y = baseline_series[keys][var].astype(float)

                if len(X) != 12 or np.isnan(X).any() or np.isnan(Y).any():
                    row[f"dtw_{col}"] = np.nan
                else:
                    row[f"dtw_{col}"] = dtw_distance(X, Y)

            rows.append(row)

    dtw_df = pd.DataFrame(rows)

    # -------------------------------------------------
    # 4. MODIFIED Z-SCORE + ROBUST NORMALIZATION
    # -------------------------------------------------
    print("Computing robust anomaly metrics...")

    for var in VARIABLES:
        col_dtw = f"dtw_{var.lower()}"

        stats = (
            dtw_df
            .groupby(group_cols)[col_dtw]
            .agg(local_median="median", local_mad=calculate_mad)
            .reset_index()
        )

        dtw_df = dtw_df.merge(stats, on=group_cols, how="left")

        mod_z = f"{col_dtw}_mod_z"
        dtw_df[mod_z] = np.where(
            dtw_df["local_mad"] == 0,
            0,
            0.6745 * (dtw_df[col_dtw] - dtw_df["local_median"]) / dtw_df["local_mad"]
        )

        dtw_df[f"{col_dtw}_index"] = (
            np.clip(np.abs(dtw_df[mod_z]), 0, SCORE_CEILING) / SCORE_CEILING
        )

        dtw_df[f"{col_dtw}_anomaly_flag"] = (
            np.abs(dtw_df[mod_z]) > ROBUST_THRESHOLD
        ).astype(int)

        dtw_df.drop(columns=["local_median", "local_mad", mod_z], inplace=True)

    # -------------------------------------------------
    # SAVE
    # -------------------------------------------------
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dtw_df.to_parquet(output_path, index=False)
    print(f"Saved → {output_path}")

# =====================================================
# RUN PIPELINES
# =====================================================
run_dtw_pipeline(
    df,
    group_cols=["province", "district"],
    output_path=OUTPUT_DISTRICT
)

run_dtw_pipeline(
    df,
    group_cols=["province"],
    output_path=OUTPUT_PROVINCE
)

print("------------------------------------------------")
print("PIPELINE FINISHED SUCCESSFULLY")
print("------------------------------------------------")
