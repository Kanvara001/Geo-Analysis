import pandas as pd
import numpy as np
import itertools
from pathlib import Path

CLEAN_DIR = Path("gee-pipeline/outputs/clean")
FILLED_DIR = Path("gee-pipeline/outputs/filled")
FILLED_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["province", "district", "subdistrict"]
THRESHOLD = 2

for var_dir in CLEAN_DIR.iterdir():
    var = var_dir.name
    print(f"💉 FILL {var}")

    dfs = [pd.read_parquet(p) for p in var_dir.glob("*.parquet")]
    df = pd.concat(dfs, ignore_index=True)

    df["date"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str) + "-01")

    # ---- build full grid ----
    full_dates = pd.date_range(df["date"].min(), df["date"].max(), freq="MS")
    areas = df[KEYS].drop_duplicates()

    grid = pd.DataFrame([
        dict(zip(KEYS, a)) | {"date": d}
        for a, d in itertools.product(areas.values, full_dates)
    ])

    df_full = grid.merge(df, on=KEYS + ["date"], how="left")
    df_full["month"] = df_full["date"].dt.month
    df_full["year"] = df_full["date"].dt.year

    # ---- fill logic ----
    def fill_group(x):
        x = x.sort_values("date")
        x[var] = x[var].interpolate(limit=THRESHOLD - 1)

        miss = x[var].isna()
        if miss.any():
            clim = x.groupby("month")[var].mean()
            x.loc[miss, var] = x.loc[miss, "month"].map(clim)

        return x

    df_full = df_full.groupby(KEYS, group_keys=False).apply(fill_group)

    # fallback → province mean
    df_full[var] = df_full.groupby("province")[var].transform(
        lambda x: x.fillna(x.mean())
    )

    out = FILLED_DIR / var
    out.mkdir(exist_ok=True)
    df_full.drop(columns="date").to_parquet(out / f"{var}_FILLED.parquet", index=False)

print("✅ FILL DONE")
