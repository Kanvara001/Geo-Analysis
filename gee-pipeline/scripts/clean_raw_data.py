import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path("gee-pipeline/outputs/raw_parquet")
CLEAN_DIR = Path("gee-pipeline/outputs/clean")
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

KEYS = ["province", "district", "subdistrict", "year", "month"]

VALUE_COLUMN_MAP = {
    # "LST": "mean",
    # "NDVI": "mean",
    # "SOILMOISTURE": "mean",
    # "RAINFALL": "sum",
    "FIRECOUNT": "sum",
}

def iqr_filter(s):
    q1, q3 = s.quantile([0.25, 0.75])
    iqr = q3 - q1
    return s.where((s >= q1 - 1.5 * iqr) & (s <= q3 + 1.5 * iqr))

for pq in RAW_DIR.rglob("*.parquet"):
    var = pq.parent.name.upper()
    print(f"🧹 CLEAN {var}: {pq.name}")

    if var not in VALUE_COLUMN_MAP:
        continue

    df = pd.read_parquet(pq)
    df = df.rename(columns={"subdistric": "subdistrict"})

    val_col = VALUE_COLUMN_MAP[var]
    df = df[KEYS + [val_col]].rename(columns={val_col: var})

    # ---------- RULES ----------
    # if var == "LST":
    #     df[var] = df[var] * 0.02 - 273.15
    #     df[var] = df[var].where(df[var].between(5, 55))
    #     df[var] = iqr_filter(df[var])

    # elif var == "NDVI":
    #     df[var] = df[var] / 10000
    #     df[var] = df[var].where(df[var].between(-0.2, 1.0))
    #     df.loc[df[var] == 0, var] = np.nan
    #     df[var] = iqr_filter(df[var])

    # elif var == "SOILMOISTURE":
    #     df[var] = df[var].where(df[var].between(0, 1))
    #     df[var] = iqr_filter(df[var])

    # elif var == "RAINFALL":
    #     df[var] = df[var].where(df[var] >= 0)

    if var == "FIRECOUNT":
        df[var] = df[var].where(df[var] >= 0)

    out = CLEAN_DIR / var
    out.mkdir(exist_ok=True)
    df.to_parquet(out / pq.name, index=False)

print("✅ CLEAN DONE (no filling yet)")
