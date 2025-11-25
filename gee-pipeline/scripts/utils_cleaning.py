import pandas as pd
import numpy as np

# ===================================================================
# Utility: Detect consecutive missing segments
# ===================================================================
def find_missing_segments(series):
    """
    Return list of (start_index, end_index, length)
    representing consecutive NaN segments.
    """
    segments = []
    in_gap = False
    start = None

    for i, is_na in enumerate(series.isna()):
        if is_na and not in_gap:
            in_gap = True
            start = i
        elif not is_na and in_gap:
            in_gap = False
            end = i - 1
            segments.append((start, end, end - start + 1))

    # If gap runs until the end
    if in_gap:
        segments.append((start, len(series) - 1, len(series) - start))

    return segments


# ===================================================================
# IQR-based Outlier Handler
# ===================================================================
def apply_iqr_outlier_removal(df, col_name):
    """
    Replace IQR outliers with NaN.
    """
    series = df[col_name]

    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    df.loc[(series < lower) | (series > upper), col_name] = np.nan
    return df


# ===================================================================
# Dual-gap Imputation
# Small gap → Interpolation
# Large gap → Same-month mean (climatology)
# ===================================================================
def impute_dual_method(df, col_name, threshold=2):
    """
    Fill missing values using:
    - small gap (< threshold): interpolation
    - large gap (>= threshold): monthly climatology
    """
    df = df.copy()

    # compute climatology (mean of same month across all years)
    df['month'] = df['date'].dt.month
    climatology = df.groupby('month')[col_name].mean()

    # find missing segments
    segments = find_missing_segments(df[col_name])

    for start, end, length in segments:
        if length < threshold:
            # SMALL GAP → interpolate
            df.loc[start:end, col_name] = np.nan
        else:
            # LARGE GAP → fill with same-month climatology
            for idx in range(start, end + 1):
                m = df.loc[idx, 'month']
                df.loc[idx, col_name] = climatology[m]

    # final interpolation pass
    df[col_name] = df[col_name].interpolate(method="linear", limit_direction="both")
    df.drop(columns=['month'], inplace=True)

    return df


# ===================================================================
# Physically plausible ranges
# ===================================================================
def apply_physical_range(df, col_name):
    """
    Clip impossible values to NaN before IQR processing.
    """
    if col_name == "LST":
        df.loc[(df[col_name] < 5) | (df[col_name] > 55), col_name] = np.nan

    elif col_name == "NDVI":
        df.loc[(df[col_name] < -0.2) | (df[col_name] > 1.0), col_name] = np.nan

    elif col_name == "SoilMoisture":
        df.loc[(df[col_name] < 0) | (df[col_name] > 1), col_name] = np.nan

    elif col_name == "Rainfall":
        df.loc[(df[col_name] < 0), col_name] = np.nan  # rain cannot be negative

    elif col_name == "Firecount":
        df.loc[(df[col_name] < 0), col_name] = np.nan

    return df


# ===================================================================
# Main Cleaner Function
# ===================================================================
def clean_variables(df):
    """
    Clean dataset with:
    - physical ranges
    - IQR outlier detection
    - dual-gap imputation (threshold=2 months)
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])

    variable_list = ["LST", "NDVI", "SoilMoisture", "Rainfall", "Firecount"]

    for var in variable_list:

        # 1) physical ranges
        df = apply_physical_range(df, var)

        # 2) IQR outlier removal
        df = apply_iqr_outlier_removal(df, var)

        # 3) dual gap imputation (threshold=2)
        df = impute_dual_method(df, var, threshold=2)

    return df

