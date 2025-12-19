import pandas as pd
import numpy as np
import itertools
from pathlib import Path

CLEAN_DIR = Path("gee-pipeline/outputs/clean")
FILLED_DIR = Path("gee-pipeline/outputs/filled")
FILLED_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["province", "district", "subdistrict"]
THRESHOLD = 2  # max gap = 1 month

for var_dir in CLEAN_DIR.iterdir():
    var = var_dir.name
    print(f"💉 FILL {var}")

    dfs = [pd.read_parquet(p) for p in var_dir.glob("*.parquet")]
    df = pd.concat(dfs, ignore_index=True)

    # build datetime
    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
    )

    # ---- build full monthly grid ----
    full_dates = pd.date_range(df["date"].min(), df["date"].max(), freq="MS")
    areas = df[KEYS].drop_duplicates()

    grid = pd.DataFrame([
        dict(zip(KEYS, a)) | {"date": d}
        for a, d in itertools.product(areas.values, full_dates)
    ])

    df_full = grid.merge(df, on=KEYS + ["date"], how="left")
    df_full["year"] = df_full["date"].dt.year
    df_full["month"] = df_full["date"].dt.month

    # ---- STEP 1 + 2: interpolate → month-mean (same area) ----
    def fill_area(x):
        x = x.sort_values("date")

        # interpolate short gaps
        x[var] = x[var].interpolate(limit=THRESHOLD - 1)

        # fill by month climatology (same area, different years)
        miss = x[var].isna()
        if miss.any():
            month_mean = x.groupby("month")[var].mean()
            x.loc[miss, var] = x.loc[miss, "month"].map(month_mean)

        return x

    df_full = df_full.groupby(KEYS, group_keys=False).apply(fill_area)

    # ---- STEP 3: province + month mean ----
    miss = df_full[var].isna()
    if miss.any():
        prov_month_mean = (
            df_full.groupby(["province", "month"])[var]
            .mean()
            .reset_index()
        )

        df_full = df_full.merge(
            prov_month_mean,
            on=["province", "month"],
            how="left",
            suffixes=("", "_prov")
        )

        df_full.loc[miss, var] = df_full.loc[miss, f"{var}_prov"]
        df_full.drop(columns=f"{var}_prov", inplace=True)

    # ---- STEP 4: final fallback → province mean ----
    df_full[var] = df_full.groupby("province")[var].transform(
        lambda x: x.fillna(x.mean())
    )

    # save
    out = FILLED_DIR / var
    out.mkdir(exist_ok=True)
    df_full.drop(columns="date").to_parquet(
        out / f"{var}_FILLED.parquet", index=False
    )

print("✅ FILL DONE (interpolate → month mean → province month → province mean)")
