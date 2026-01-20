import pandas as pd
import numpy as np
from scipy.stats import median_abs_deviation
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
INPUT_PATH = "gee-pipeline/outputs/merged/merged_dataset_FILLED.parquet"
OUTPUT_PATH = "gee-pipeline/outputs/merged/dtw_results_robust.parquet"

VARIABLES = ["NDVI", "RAINFALL", "SOILMOISTURE", "LST", "FIRECOUNT"]

# Threshold ตามงานวิจัย Robust Anomaly Detection (Iglewicz & Hoaglin, 1993)
# ค่า 3.5 ถือเป็นจุดตัดมาตรฐานสำหรับ Modified Z-score
ROBUST_THRESHOLD = 3.5 

# -----------------------------
# DTW FUNCTIONS (Standard)
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
# HELPER: ROBUST STATS
# -----------------------------
def calculate_mad(x):
    # scale='normal' จะคูณด้วย 1.4826 (1/0.6745) ให้โดยอัตโนมัติ
    # เพื่อให้ MAD มีสเกลเทียบเท่า Standard Deviation
    return median_abs_deviation(x, scale='normal')

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
# 1. ROBUST BASELINE (Median per month per SUBDISTRICT)
# -----------------------------
# เปลี่ยนจาก trim_mean เป็น median เพื่อความทนทานต่อ outlier สูงสุด
print("Computing robust baseline (Median)...")
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
            # ใช้ Median ของเดือนนั้นๆ จากข้อมูลประวัติทั้งหมด
            vals = group[group["month"] == m][col].dropna().values
            val_median = np.median(vals) if len(vals) > 0 else np.nan
            monthly_baseline.append(val_median)

        baseline_series[key][var] = np.array(monthly_baseline)

# -----------------------------
# 2. DTW CALCULATION
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

        # ---- baseline values (Optional: เก็บไว้ดูเทียบ) ----
        for var in VARIABLES:
            baseline_vals = baseline_series[key][var]
            for m in range(12):
                row[f"baseline_{var.lower()}_m{m+1:02d}"] = baseline_vals[m]

        # ---- DTW Distance ----
        for var in VARIABLES:
            col = var.lower()
            X = year_group[col].values.astype(float) # ข้อมูลปีปัจจุบัน
            Y = baseline_series[key][var].astype(float) # Baseline (Median Profile)

            # ตรวจสอบความสมบูรณ์ของข้อมูล
            if len(X) != 12 or np.isnan(X).any() or np.isnan(Y).any():
                dist = np.nan
            else:
                dist = dtw_distance(X, Y)

            row[f"dtw_{col}"] = dist

        results.append(row)

dtw_df = pd.DataFrame(results)

# -----------------------------
# 3. ROBUST NORMALIZATION (Modified Z-score)
# -----------------------------
print("Computing Robust Statistics (Median & MAD) and Anomaly Scores...")

for var in VARIABLES:
    col = f"dtw_{var.lower()}"
    
    # คำนวณ Median และ MAD ของค่า DTW ในแต่ละตำบล (Subdistrict)
    # เพื่อสร้าง Distribution Profile ของความผิดปกติในพื้นที่นั้น
    stats = (
        dtw_df
        .groupby(["district", "subdistrict"])[col]
        .agg(
            local_median="median", 
            local_mad=calculate_mad
        )
        .reset_index()
        .rename(columns={
            "local_median": f"{col}_median",
            "local_mad": f"{col}_mad"
        })
    )

    # Merge ค่าสถิติกลับเข้าไป
    dtw_df = dtw_df.merge(stats, on=["district", "subdistrict"], how="left")

    # คำนวณ Modified Z-score
    # สูตร: 0.6745 * (x - median) / MAD
    # หมายเหตุ: ฟังก์ชัน calculate_mad ด้านบนใส่ scale='normal' ไว้แล้ว
    # ซึ่งเทียบเท่ากับหารด้วย 1/0.6745 ดังนั้นเราแค่หารด้วย MAD ได้เลย
    
    # ป้องกันการหารด้วย 0 กรณี MAD เป็น 0 (ข้อมูลนิ่งมาก)
    dtw_df[f"{col}_mod_z"] = np.where(
        dtw_df[f"{col}_mad"] == 0,
        0, # หรือ np.nan ขึ้นอยู่กับว่าอยากจัดการยังไง ถ้า MAD=0 แปลว่าข้อมูลเหมือนเดิมตลอด
        (dtw_df[col] - dtw_df[f"{col}_median"]) / dtw_df[f"{col}_mad"]
    )

# -----------------------------
# 4. ANOMALY FLAGGING (Robust Threshold)
# -----------------------------
print(f"Applying Robust Threshold (> {ROBUST_THRESHOLD})...")

for var in VARIABLES:
    col = f"dtw_{var.lower()}"
    # เช็คค่า Absolute ของ Z-score เพราะผิดปกติอาจจะมากไปหรือน้อยไปก็ได้
    # แต่ปกติ DTW ยิ่งมากยิ่งผิดปกติ ดังนั้นดูค่าบวกอย่างเดียวน่าจะ make sense กว่าในบริบทนี้
    # แต่ถ้าใช้ Z-score ปกติจะดู 2 ฝั่ง (ในที่นี้ DTW min คือ 0 ดังนั้นดูค่าบวกอย่างเดียวคือถูกต้องแล้ว)
    dtw_df[f"{col}_anomaly_flag"] = (dtw_df[f"{col}_mod_z"] > ROBUST_THRESHOLD).astype(int)

# -----------------------------
# CLEANUP & SAVE
# -----------------------------
print("Dropping temporary columns...")
drop_cols = []
for var in VARIABLES:
    col = f"dtw_{var.lower()}"
    drop_cols += [f"{col}_median", f"{col}_mad"]

dtw_df = dtw_df.drop(columns=drop_cols)

Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
dtw_df.to_parquet(OUTPUT_PATH, index=False)

print("Robust DTW Anomaly Detection finished.")
print(f"Saved to {OUTPUT_PATH}")
