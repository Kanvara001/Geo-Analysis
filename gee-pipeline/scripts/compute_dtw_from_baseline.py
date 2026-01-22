import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"
# เปลี่ยนชื่อไฟล์ output เป็น normalized ให้ตรงกับ workflow
OUTPUT_PATH = "gee-pipeline/outputs/merged/dtw_results_normalized.parquet" 

VARIABLES = ["NDVI", "RAINFALL", "SOILMOISTURE", "LST", "FIRECOUNT"]

ROBUST_THRESHOLD = 3.5 
SCORE_CEILING = 5.0 

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

# -----------------------------
# 1. COMPUTE ROBUST BASELINE (Median)
# -----------------------------
print("Computing robust baseline (Median per Subdistrict)...")
baseline_series = {}

# Group by Subdistrict เพื่อหาค่ากลางของพื้นที่นั้นๆ
for (province, district, subdistrict), group in df.groupby(["province", "district", "subdistrict"]):
    key = (province, district, subdistrict)
    baseline_series[key] = {}
    
    for var in VARIABLES:
        col = var.lower()
        # ดึงข้อมูลแยกรายเดือน (m=1 ถึง 12) มาหา Median
        vals = [group[group["month"] == m][col].dropna().values for m in range(1, 13)]
        
        # สร้าง array 12 ค่า (Jan-Dec) ที่เป็นค่าปกติของพื้นที่นี้
        baseline_series[key][var] = np.array([np.median(v) if len(v) > 0 else np.nan for v in vals])

# -----------------------------
# 2. COMPUTE DTW & STORE BASELINE
# -----------------------------
print("Computing DTW distances & Storing Baselines...")
results = []

for (province, district, subdistrict), group in df.groupby(["province", "district", "subdistrict"]):
    key = (province, district, subdistrict)
    
    # วนลูปทีละปี เพื่อคำนวณว่าปีนั้นๆ เพี้ยนไปจาก Baseline แค่ไหน
    for year, year_group in group.groupby("year"):
        year_group = year_group.sort_values("month")
        row = {
            "province": province, 
            "district": district, 
            "subdistrict": subdistrict, 
            "year": year
        }

        # --- ส่วนที่ 1: เก็บค่า Baseline ลงไปด้วย (12 เดือน x 5 ตัวแปร) ---
        for var in VARIABLES:
            baseline_vals = baseline_series[key][var]
            for m in range(12):
                # ชื่อ column เช่น: baseline_ndvi_m01, baseline_lst_m12
                row[f"baseline_{var.lower()}_m{m+1:02d}"] = baseline_vals[m]

        # --- ส่วนที่ 2: คำนวณ DTW ---
        for var in VARIABLES:
            col = var.lower()
            X = year_group[col].values.astype(float)      # ข้อมูลจริงปีนั้น (12 เดือน)
            Y = baseline_series[key][var].astype(float)   # ข้อมูล Baseline (12 เดือน)
            
            # เช็คความสมบูรณ์ของข้อมูลก่อนคำนวณ
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
    
    # 3.1 Calculate Stats (Median & MAD ของ DTW ในพื้นที่นั้น)
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
    
    # 3.3 Convert to Index 0-1
    index_col = f"{col_dtw}_index"
    # Clip 0-5.0 แล้วหาร 5.0
    dtw_df[index_col] = np.clip(dtw_df[mod_z_col], 0, SCORE_CEILING) / SCORE_CEILING
    
    # 3.4 Flag (>3.5)
    dtw_df[f"{col_dtw}_anomaly_flag"] = (dtw_df[mod_z_col] > ROBUST_THRESHOLD).astype(int)

    # Clean up temp cols (ลบ Mod Z ออก เก็บไว้แค่ index, flag และ baseline)
    dtw_df.drop(columns=["local_median", "local_mad", mod_z_col], inplace=True)

# -----------------------------
# SAVE
# -----------------------------
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("------------------------------------------------")
print(f"Saved to: {OUTPUT_PATH}")
print(f"Columns Example:")
print(f" - Scores: dtw_ndvi_index (0.0-1.0)")
print(f" - Baseline: baseline_ndvi_m01 ... baseline_ndvi_m12")
print("------------------------------------------------")
