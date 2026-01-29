import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
from pathlib import Path

# =====================================================
# CONFIGURATION
# =====================================================
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"

OUTPUT_DISTRICT = "gee-pipeline/outputs/aggregated/district_dtw_results.parquet"
OUTPUT_PROVINCE = "gee-pipeline/outputs/aggregated/province_dtw_results.parquet"

VARIABLES = ["NDVI", "RAINFALL", "SOILMOISTURE", "LST", "FIRECOUNT"]

# Reference: Iglewicz & Hoaglin (1993) / Zhu & Woodcock (2014)
# 3.5 is a conservative threshold for small/moderate datasets to avoid false alarms.
ROBUST_THRESHOLD = 3.5 

# Reference: OECD (2008) - Winsorization ceiling for index construction
SCORE_CEILING = 5.0

# =====================================================
# CORE FUNCTIONS
# =====================================================
def compute_cost_matrix(X, Y):
    """Computes the Euclidean distance matrix between two time series."""
    N, M = len(X), len(Y)
    C = np.zeros((N, M))
    for i in range(N):
        for j in range(M):
            C[i, j] = abs(X[i] - Y[j])
    return C

def dtw_distance(X, Y):
    """
    Computes Dynamic Time Warping (DTW) distance.
    Reference: Keogh & Ratanamahatana (2005)
    """
    C = compute_cost_matrix(X, Y)
    N, M = C.shape
    D = np.full((N + 1, M + 1), np.inf)
    D[0, 0] = 0

    for i in range(1, N + 1):
        for j in range(1, M + 1):
            D[i, j] = C[i - 1, j - 1] + min(
                D[i - 1, j],    # Insertion
                D[i, j - 1],    # Deletion
                D[i - 1, j - 1] # Match
            )
    return D[N, M]

def calculate_raw_mad(x):
    """
    Calculates the raw Median Absolute Deviation (MAD).
    scale=1 is CRITICAL here because we apply the 0.6745 factor manually later.
    """
    return median_abs_deviation(x, scale=1) 

# =====================================================
# LOAD DATA
# =====================================================
print(f"Loading dataset from: {INPUT_PATH}")
df = pd.read_parquet(INPUT_PATH)
df.columns = df.columns.str.strip().str.lower()

# =====================================================
# MAIN PIPELINE
# =====================================================
def run_dtw_pipeline(df, group_cols, output_path):
    """
    Runs the full Anomaly Detection Pipeline:
    1. Aggregation -> Monthly Mean
    2. Baseline -> Monthly Median (Robust)
    3. DTW Calculation
    4. Modified Z-Score (Iglewicz & Hoaglin)
    5. Index Normalization (0-1)
    """
    level_name = "District" if "district" in group_cols else "Province"
    print(f"\n--- Processing Level: {level_name} ---")

    # -------------------------------------------------
    # 1. AGG TO MONTHLY (MEAN)
    # -------------------------------------------------
    print("Step 1: Aggregating monthly data...")
    agg_dict = {v.lower(): "mean" for v in VARIABLES}
    
    monthly = (
        df
        .groupby(group_cols + ["year", "month"], as_index=False)
        .agg(agg_dict)
    )

    # -------------------------------------------------
    # 2. COMPUTE ROBUST BASELINE (Median Jan–Dec)
    # Reference: Leys et al. (2013)
    # -------------------------------------------------
    print("Step 2: Computing robust baseline (Median)...")
    baseline_series = {}

    for keys, group in monthly.groupby(group_cols):
        baseline_series[keys] = {}

        for var in VARIABLES:
            col = var.lower()
            # Collect data for each month (1-12) across all years
            monthly_vals = [
                group[group["month"] == m][col].dropna().values
                for m in range(1, 13)
            ]
            
            # Calculate Median for each month
            baseline_series[keys][var] = np.array(
                [np.median(v) if len(v) > 0 else np.nan for v in monthly_vals]
            )

    # -------------------------------------------------
    # 3. COMPUTE DTW PER YEAR
    # Reference: Keogh & Ratanamahatana (2005)
    # -------------------------------------------------
    print("Step 3: Calculating DTW distances...")
    rows = []

    for keys, group in monthly.groupby(group_cols):
        # Identify key columns (handle single vs multiple keys)
        key_values = keys if isinstance(keys, tuple) else [keys]
        base_info = {col: val for col, val in zip(group_cols, key_values)}

        for year, year_group in group.groupby("year"):
            year_group = year_group.sort_values("month")

            row = base_info.copy()
            row["year"] = year

            # (Optional) Store baseline values for reference
            for var in VARIABLES:
                base = baseline_series[keys][var]
                for m in range(12):
                    row[f"baseline_{var.lower()}_m{m+1:02d}"] = base[m]

            # Calculate DTW for each variable
            for var in VARIABLES:
                col = var.lower()
                X = year_group[col].values.astype(float)
                Y = baseline_series[keys][var].astype(float)

                # Validate data completeness (must have 12 months)
                if len(X) != 12 or np.isnan(X).any() or np.isnan(Y).any():
                    row[f"dtw_{col}"] = np.nan
                else:
                    row[f"dtw_{col}"] = dtw_distance(X, Y)

            rows.append(row)

    dtw_df = pd.DataFrame(rows)

    # -------------------------------------------------
    # 4. MODIFIED Z-SCORE + NORMALIZATION
    # Reference: Iglewicz & Hoaglin (1993) for Mod-Z
    # Reference: OECD (2008) for Winsorization
    # -------------------------------------------------
    print("Step 4: Computing Anomaly Scores & Indexing...")

    for var in VARIABLES:
        col_dtw = f"dtw_{var.lower()}"

        # Calculate Median and Raw MAD per location (group_cols)
        stats = (
            dtw_df
            .groupby(group_cols)[col_dtw]
            .agg(local_median="median", local_mad=calculate_raw_mad)
            .reset_index()
        )

        dtw_df = dtw_df.merge(stats, on=group_cols, how="left")

        # --- A. Modified Z-Score Calculation ---
        # Formula: M_i = 0.6745 * (x - median) / MAD
        mod_z = f"{col_dtw}_mod_z"
        dtw_df[mod_z] = np.where(
            dtw_df["local_mad"] == 0,
            0, # Avoid division by zero if MAD is 0
            0.6745 * (dtw_df[col_dtw] - dtw_df["local_median"]) / dtw_df["local_mad"]
        )

        # --- B. Index Normalization (0-1) ---
        # Clip at ceiling (Winsorization) then Scale
        dtw_df[f"{col_dtw}_index"] = (
            np.clip(np.abs(dtw_df[mod_z]), 0, SCORE_CEILING) / SCORE_CEILING
        )

        # --- C. Anomaly Flagging ---
        # Threshold > 3.5 (Conservative)
        dtw_df[f"{col_dtw}_anomaly_flag"] = (
            np.abs(dtw_df[mod_z]) > ROBUST_THRESHOLD
        ).astype(int)

        # Cleanup temporary columns
        dtw_df.drop(columns=["local_median", "local_mad", mod_z], inplace=True)

    # -------------------------------------------------
    # SAVE OUTPUT
    # -------------------------------------------------
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dtw_df.to_parquet(output_path, index=False)
    print(f"✅ Saved results to: {output_path}")

# =====================================================
# EXECUTION
# =====================================================
# 1. District Level
run_dtw_pipeline(
    df,
    group_cols=["province", "district"],
    output_path=OUTPUT_DISTRICT
)

# 2. Province Level
run_dtw_pipeline(
    df,
    group_cols=["province"],
    output_path=OUTPUT_PROVINCE
)

print("\n" + "="*50)
print("PIPELINE COMPLETED SUCCESSFULLY")
print("="*50)
