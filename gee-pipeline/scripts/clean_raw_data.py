import os
import glob
import pandas as pd

PARQUET_DIR = "gee-pipeline/outputs/raw_parquet"
OUT_CSV = "gee-pipeline/processed/monthly_clean.csv"

os.makedirs("gee-pipeline/processed", exist_ok=True)

# prefix mapping
VAR_MAP = {
    "NDVI": "ndvi",
    "LST": "lst",
    "Rainfall": "rain",
    "SoilMoisture": "soil",
    "FireCount": "fire"
}

def detect_var(path):
    """Infer variable name from filename prefix."""
    for key in VAR_MAP:
        if key.lower() in path.lower():
            return VAR_MAP[key]
    return None


def main():
    print("🔍 Loading Parquet files...")

    files = glob.glob(f"{PARQUET_DIR}/*.parquet")
    if not files:
        print("❌ No Parquet files found.")
        return

    merged = None

    for f in files:
        var = detect_var(f)
        if var is None:
            print(f"⚠ Skip (unknown variable): {f}")
            continue

        print(f"📥 Reading {var}: {f}")
        df = pd.read_parquet(f)

        # rename "value" column → variable name
        df = df.rename(columns={"value": var})

        # merge by lat/lon
        if merged is None:
            merged = df
        else:
            merged = merged.merge(df, on=["lat", "lon"], how="outer")

    if merged is None:
        print("❌ No usable datasets found.")
        return

    print(f"💾 Saving merged CSV → {OUT_CSV}")
    merged.to_csv(OUT_CSV, index=False)

    print("🎉 Done! monthly_clean.csv generated.")


if __name__ == "__main__":
    main()
