import pandas as pd
import numpy as np
import itertools
from pathlib import Path

CLEAN_DIR = Path("gee-pipeline/outputs/clean")
FILLED_DIR = Path("gee-pipeline/outputs/filled")
FILLED_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["province", "district", "subdistrict"]
TIME_KEYS = ["year", "month"]
THRESHOLD = 2  # interpolate gap ≤ 1 เดือน

print("🚑 FINAL FILL — START")

for var_dir in CLEAN_DIR.iterdir():
    if not var_dir.is_dir():
        continue

    var = var_dir.name
    print(f"\n💉 Filling {var}")

    # -------------------------------
    # Load
    # -------------------------------
    dfs = [pd.read_parquet(p) for p in var_dir.glob("*.parquet")]
    df = pd.concat(dfs, ignore_index=True)

    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
    )

    # -------------------------------
    # Build full monthly grid
    # -------------------------------
    full_dates = pd.date_range(df["date"].min(), df["date"].max(), freq="MS")
    areas = df[KEYS].drop_duplicates()

    grid = pd.DataFrame([
        dict(zip(KEYS, a)) | {"date": d}
        for a, d in itertools.product(areas.values, full_dates)
    ])

    df_full = grid.merge(df, on=KEYS + ["date"], how="left")
    df_full["year"] = df_full["date"].dt.year
    df_full["month"] = df_full["date"].dt.month

    # -------------------------------
    # FILL LOGIC (CORE)
    # -------------------------------
    def fill_area(x):
        x = x.sort_values("date")

        # 1️⃣ interpolate (short gap only)
        x[var] = x[var].interpolate(limit=THRESHOLD - 1)

        # 2️⃣ same month mean (other years, same area)
        miss = x[var].isna()
        if miss.any():
            month_mean = x.groupby("month")[var].mean()
            x.loc[miss, var] = x.loc[miss, "month"].map(month_mean)

        # 3️⃣ area mean (all time)
        if x[var].isna().any():
            x[var] = x[var].fillna(x[var].mean())

        return x

    df_full = (
        df_full
        .groupby(KEYS, group_keys=False)
        .apply(fill_area)
    )

    # -------------------------------
    # 4️⃣ province fallback
    # -------------------------------
    df_full[var] = df_full.groupby("province")[var].transform(
        lambda x: x.fillna(x.mean())
    )

    # -------------------------------
    # Save
    # -------------------------------
    out_dir = FILLED_DIR / var
    out_dir.mkdir(exist_ok=True)

    df_full.drop(columns="date").to_parquet(
        out_dir / f"{var}_FILLED.parquet",
        index=False
    )

    print(f"✅ {var} done | NaN left = {df_full[var].isna().sum()}")

print("\n🏁 FINAL FILL COMPLETED")
