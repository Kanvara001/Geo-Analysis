import os
import json
import pandas as pd
from datetime import datetime

RAW_PARQUET_DIR = "gee-pipeline/raw_parquet"
CLEAN_OUTPUT_DIR = "gee-pipeline/clean_parquet"

os.makedirs(CLEAN_OUTPUT_DIR, exist_ok=True)

# -----------------------------
# Helper: safe rename columns
# -----------------------------
COLUMN_MAP = {
    "amphoe": "district",
    "tambon": "subdistrict",
    "district": "district",
    "subdistrict": "subdistrict",
    "province": "province",
}

def normalize_columns(df):
    df = df.rename(columns={c: COLUMN_MAP.get(c, c) for c in df.columns})
    return df

# -----------------------------
# Clean one variable
# -----------------------------
def clean_variable(df, var):
    temp = df[df["variable"] == var].copy()
    if temp.empty:
        print(f"⚠ No data for variable: {var}")
        return None

    # Normalize columns
    temp = normalize_columns(temp)

    # Make sure essential columns exist
    for col in ["province", "district", "subdistrict"]:
        if col not in temp.columns:
            temp[col] = None

    # Create date column
    temp["date"] = pd.to_datetime(
        temp["year"].astype(str) + "-" + temp["month"].astype(str) + "-01"
    )

    # Sort safely
    sort_cols = [c for c in ["province", "district", "subdistrict", "date"] if c in temp.columns]
    temp = temp.sort_values(sort_cols)

    # Rename the value column: always called "value"
    if "mean" in temp.columns:
        temp["value"] = temp["mean"]
    elif "sum" in temp.columns:
        temp["value"] = temp["sum"]
    elif "FireMask" in temp.columns:
        temp["value"] = temp["FireMask"]
    else:
        # fallback: numeric columns
        numeric_cols = temp.select_dtypes(include="number").columns.tolist()
        if "value" not in temp.columns and len(numeric_cols) > 0:
            temp["value"] = temp[numeric_cols[0]]

    # FireCount cleanup (convert >7 = fire pixel)
    if var == "FireCount":
        temp["value"] = temp["value"].apply(lambda x: 1 if x >= 7 else 0)

    # Keep only necessary fields
    keep = ["province", "district", "subdistrict",
            "year", "month", "date", "variable", "value"]

    keep = [c for c in keep if c in temp.columns]
    temp = temp[keep]

    return temp

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    print("🔧 Cleaning raw data…")

    # Load all parquet
    files = [f for f in os.listdir(RAW_PARQUET_DIR) if f.endswith(".parquet")]
    print(f"📦 Loaded {len(files)} parquet files")

    dfs = []
    for f in files:
        dfs.append(pd.read_parquet(os.path.join(RAW_PARQUET_DIR, f)))

    df = pd.concat(dfs, ignore_index=True)
    print(f"📊 Total rows: {len(df):,}")

    VARIABLES = df["variable"].unique()

    for var in VARIABLES:
        print(f"\n✨ Cleaning variable: {var}")
        cleaned = clean_variable(df, var)

        if cleaned is None or cleaned.empty:
            print(f"⚠ Skipped {var} (no data)")
            continue

        out_path = os.path.join(CLEAN_OUTPUT_DIR, f"{var}.parquet")
        cleaned.to_parquet(out_path, index=False)
        print(f"✅ Saved cleaned: {out_path} ({len(cleaned):,} rows)")

    print("\n🎉 Finished cleaning all variables")
