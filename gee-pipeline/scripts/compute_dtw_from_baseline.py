import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"
OUTPUT_PATH = "gee-pipeline/outputs/merged/dtw_results_normalized.parquet" # ตั้งชื่อใหม่เป็น normalized

VARIABLES = ["NDVI", "RAINFALL", "SOILMOISTURE", "LST", "FIRECOUNT"]

# Threshold สำหรับ Flag (ยังคงไว้ที่ 3.5 ตามสถิติ)
ROBUST_THRESHOLD = 3.5 

# *** CEILING ***
# ค่า Mod Z ที่ >= 5.0 จะถูกบีบให้เท่ากับ 1.0 (ค่าสูงสุด)
SCORE_CEILING = 5.0 

# -----------------------------
# FUNCTIONS (DTW & MAD) - เหมือนเดิม
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
            D[i, j] = C[i - 1, j - 1] + min(D[i - 1, j], D[i, j - 1], D[i - 1, j - 1])
    return D[N, M]

def calculate_mad(x):
    return median_abs_deviation(x, scale='normal')

# -----------------------------
# LOAD & PROCESS
# -----------------------------
print("Loading dataset...")
df = pd.read_parquet(INPUT_PATH)
df.columns = df.columns.str.strip().str.lower()

# 1. BASELINE
print("Computing robust baseline (Median)...")
baseline_series = {}
for (province, district, subdistrict), group in df.groupby(["province", "district", "subdistrict"]):
    key = (province, district, subdistrict)
    baseline_series[key] = {}
    for var in VARIABLES:
        col = var.lower()
        vals = [group[group["month"] == m][col].dropna().values for m in range(1, 13)]
        baseline_series[key][var] = np.array([np.median(v) if len(v) > 0 else np.nan for v in vals])

# 2. DTW
print("Computing DTW distances...")
results = []
for (province, district, subdistrict), group in df.groupby(["province", "district", "subdistrict"]):
    key = (province, district, subdistrict)
    for year, year_group in group.groupby("year"):
        year_group = year_group.sort_values("month")
        row = {"province": province, "district": district, "subdistrict": subdistrict, "year": year}
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
# 3. ROBUST NORMALIZATION (0-1 Scale)
# -----------------------------
print("Computing Anomaly Index (0.0 - 1.0)...")

for var in VARIABLES:
    col_dtw = f"dtw_{var.lower()}"
    
    # 3.1 Calculate Stats
    stats = dtw_df.groupby(["district", "subdistrict"])[col_dtw].agg(
        local_median="median", 
        local_mad=calculate_mad
    ).reset_index()
    
    dtw_df = dtw_df.merge(stats, on=["district", "subdistrict"], how="left")
    
    # 3.2 Calculate Mod Z
    mod_z_col = f"{col_dtw}_mod_z"
    dtw_df[mod_z_col] = np.where(
        dtw_df["local_mad"] == 0, 0, 
        (dtw_df[col_dtw] - dtw_df["local_median"]) / dtw_df["local_mad"]
    )
    
    # 3.3 *** แปลงเป็น 0-1 (Anomaly Index) ***
    index_col = f"{col_dtw}_index" # เปลี่ยนชื่อต่อท้ายเป็น _index ให้สื่อความหมาย
    
    # Logic:
    # 1. Clip ให้ค่าอยู่ระหว่าง 0 ถึง 5.0 
    #    (เราสนใจแค่ด้านมากผิดปกติ ดังนั้นค่าติดลบให้เป็น 0, ค่าเกิน 5 ให้เป็น 5)
    # 2. หารด้วย 5.0 เพื่อแปลงเป็น 0-1
    dtw_df[index_col] = np.clip(dtw_df[mod_z_col], 0, SCORE_CEILING) / SCORE_CEILING
    
    # 3.4 Flag (ยังใช้ Threshold 3.5 เหมือนเดิม ซึ่งถ้าแปลงเป็น index จะเท่ากับ 0.7)
    dtw_df[f"{col_dtw}_anomaly_flag"] = (dtw_df[mod_z_col] > ROBUST_THRESHOLD).astype(int)

    # Clean up temp cols
    dtw_df.drop(columns=["local_median", "local_mad", mod_z_col], inplace=True)

# -----------------------------
# SAVE
# -----------------------------
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("------------------------------------------------")
print(f"Saved to: {OUTPUT_PATH}")
print("New columns: dtw_ndvi_index, dtw_rainfall_index (Range 0.0 - 1.0)")
print("Interpretation:")
print("  0.0 - 0.4 : Normal")
print("  0.4 - 0.7 : Warning")
print("  0.7 - 1.0 : Critical Anomaly")
print("------------------------------------------------")
