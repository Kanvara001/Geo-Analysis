import os
import glob
import pandas as pd

RAW_CSV_DIR = "gee-pipeline/raw_export"
PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUTPUT_CLEAN = "gee-pipeline/processed/monthly_clean.csv"

print("🔍 Checking raw_export/ for CSV files...")

# 1) หาไฟล์ CSV ก่อน
csv_files = glob.glob(f"{RAW_CSV_DIR}/*.csv")

if len(csv_files) == 0:
    print("ℹ No CSV found. Converting Parquet → CSV automatically…")

    parquet_files = glob.glob(f"{PARQUET_DIR}/*.parquet")

    if len(parquet_files) == 0:
        print("❌ No Parquet files found in outputs/raw_parquet/. Cannot continue.")
        exit(1)

    # โหลดทุก parquet รวมเป็น dataframe เดียว
    frames = []
    for fp in parquet_files:
        print(f"   → Loading {fp}")
        frames.append(pd.read_parquet(fp))

    full = pd.concat(frames, ignore_index=True)

    # สร้างโฟลเดอร์ก่อนเซฟ
    os.makedirs(RAW_CSV_DIR, exist_ok=True)

    tmp_csv = f"{RAW_CSV_DIR}/auto_from_parquet.csv"
    full.to_csv(tmp_csv, index=False)
    print(f"✔ Parquet converted → {tmp_csv}")

    csv_files = [tmp_csv]

# 2) โหลด CSV ที่ได้มาทั้งหมด
print("📑 Loading CSV files…")

dfs = [pd.read_csv(fp) for fp in csv_files]

full_df = pd.concat(dfs, ignore_index=True)

# 3) ทำความสะอาดข้อมูลตามที่ต้องการ
print("🧹 Cleaning data…")
full_df = full_df.drop_duplicates()

# 4) เซฟเป็น monthly_clean.csv
os.makedirs("gee-pipeline/processed", exist_ok=True)
full_df.to_csv(OUTPUT_CLEAN, index=False)

print(f"✅ Cleaning completed → {OUTPUT_CLEAN}")
