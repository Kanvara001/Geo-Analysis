#!/usr/bin/env python3
"""
clean_raw_data.py
- Read raw parquet files produced by poll_download_convert.py
- Aggregate to monthly per tambon where needed
- Remove outliers (IQR)
- Gap-fill according to rules:
    NDVI/LST: long gap >= 2 months -> mean(same month other years)
              else -> linear interpolation
    SMAP: long gap >= 1 month -> mean(same month other years)
          else -> linear interpolation
    RAIN/FIRE: no filling (assume counts and sums are valid)
- Output: gee-pipeline/outputs/clean/cleaned_combined_{START}_{END}.parquet
- Column order: year,month,province,amphoe,tambon,variable,value
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).parents[1]
RAW_PARQUET = BASE / "outputs" / "raw_parquet"
CLEAN_OUT = BASE / "outputs" / "clean"
CLEAN_OUT.mkdir(parents=True, exist_ok=True)

# Parameters
START_YEAR = int(os.environ.get("START_YEAR", 2015))
END_YEAR = int(os.environ.get("END_YEAR", 2024))

# Helper: read all parquet into a dataframe with var/year/month extracted from path
def load_all_raw():
    records = []
    for p in RAW_PARQUET.rglob("*.parquet"):
        try:
            df = pd.read_parquet(p)
        except Exception as e:
            print("Skipping file (read error):", p, e)
            continue
        # try to infer var/year/month from path: raw_parquet/VAR/YYYY/MM/filename.parquet
        parts = p.parts
        # find index of 'raw_parquet' then get next 3 parts
        try:
            i = parts.index('raw_parquet')
            var = parts[i+1]
            year = parts[i+2]
            month = parts[i+3]
        except Exception:
            var = "UNKNOWN"
            year = None
            month = None
        # normalize columns: province/amphoe/tambon names may vary; try to detect
        # common fields: 'ADM1_NAME' etc — but we expect the exported tambon FC includes properties named province/amphoe/tambon
        # try multiple possibilities
        col_map = {}
        for c in df.columns:
            low = c.lower()
            if 'province' in low or 'adm1' in low:
                col_map[c] = 'province'
            if 'amphoe' in low or 'district' in low or 'adm2' in low:
                col_map[c] = 'amphoe'
            if 'tambon' in low or 'subdistrict' in low or 'adm3' in low:
                col_map[c] = 'tambon'
            if low in ('mean','ndvi','value','avg'):
                # choose first numeric col as value if band name not present
                if 'value' not in col_map.values():
                    col_map[c] = 'value'
        # find first numeric column if no value detected
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if 'value' not in col_map.values():
            if len(numeric_cols)>0:
                col_map[numeric_cols[0]] = 'value'
        df = df.rename(columns=col_map)
        # ensure province/amphoe/tambon exist
        for k in ['province','amphoe','tambon']:
            if k not in df.columns:
                df[k] = None
        # add metadata
        df['__var'] = var
        df['__year'] = int(year) if year else None
        df['__month'] = int(month) if month else None
        # if there's image_date column, try to parse for more accurate year/month
        if 'image_date' in df.columns:
            try:
                df['image_date'] = pd.to_datetime(df['image_date'])
                df['__year'] = df['image_date'].dt.year
                df['__month'] = df['image_date'].dt.month
            except Exception:
                pass
        records.append(df[['province','amphoe','tambon','value','__var','__year','__month']])
    if len(records)==0:
        return pd.DataFrame(columns=['province','amphoe','tambon','value','__var','__year','__month'])
    all_df = pd.concat(records, ignore_index=True, sort=False)
    # ensure types
    all_df['__year'] = pd.to_numeric(all_df['__year'], errors='coerce').astype('Int64')
    all_df['__month'] = pd.to_numeric(all_df['__month'], errors='coerce').astype('Int64')
    return all_df

print("Loading raw parquet files...")
raw = load_all_raw()
if raw.empty:
    print("No raw parquet files found under", RAW_PARQUET)
    exit(0)

# Normalize tambon names: strip
for c in ['province','amphoe','tambon']:
    raw[c] = raw[c].astype(str).str.strip()

# Keep only rows where year/month present
raw = raw.dropna(subset=['__year','__month','value'])

# Aggregate to monthly per tambon if raw contains multiple rows for same month (e.g., daily)
print("Aggregating raw values to monthly per tambon per variable...")
# For rainfall, we want monthly sum; for others monthly mean
def agg_monthly(group):
    var = group.name[2]  # tuple (province,amphoe,tambon, var)
    if var == 'RAIN':
        return group['value'].sum()
    elif var == 'FIRE':
        return group['value'].sum()
    else:
        return group['value'].mean()

# pivot/ group
agg = raw.groupby(['province','amphoe','tambon','__var','__year','__month'], dropna=False)['value'].agg(
    lambda x: x.sum() if x.name == 'value' else x.mean()
).reset_index()

# But above lambda isn't correct for knowing var. We'll do manual:
rows = []
for (prov, amp, tam, var, yr, mo), g in raw.groupby(['province','amphoe','tambon','__var','__year','__month']):
    if var == 'RAIN' or var == 'FIRE':
        v = g['value'].sum(skipna=True)
    else:
        v = g['value'].mean(skipna=True)
    rows.append({'province':prov,'amphoe':amp,'tambon':tam,'variable':var,'year':int(yr),'month':int(mo),'value':float(v) if pd.notna(v) else np.nan})
monthly = pd.DataFrame(rows)

# ---- Outlier removal (IQR) per tambon-variable across time ----
print("Applying IQR outlier removal per tambon-variable...")
def remove_iqr_outliers(df, col='value'):
    out = []
    grouped = df.groupby(['province','amphoe','tambon','variable'])
    for (prov, amp, tam, var), g in grouped:
        vals = g[col].dropna()
        if len(vals) < 5:
            # not enough points to compute IQR reliably
            out.append(g)
            continue
        q1 = vals.quantile(0.25)
        q3 = vals.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        g.loc[(g[col] < lower) | (g[col] > upper), col] = np.nan
        out.append(g)
    return pd.concat(out, ignore_index=True)

monthly_clean = remove_iqr_outliers(monthly)

# ---- Pivot to time-series per tambon-variable for gap filling ----
print("Preparing time series for gap filling...")
# Create full month index
periods = pd.period_range(start=f"{START_YEAR}-01", end=f"{END_YEAR}-12", freq='M')
periods_dt = [p.to_timestamp() for p in periods]

# Build dict keyed by (prov,amp,tam,variable) -> pd.Series indexed by period start (Timestamp)
ts_dict = {}
grouped = monthly_clean.groupby(['province','amphoe','tambon','variable'])
for (prov, amp, tam, var), g in grouped:
    s = pd.Series(index=pd.to_datetime([f"{int(r['year'])}-{int(r['month'])}-01" for _,r in g.iterrows()]), data=g['value'].values)
    # reindex to full periods
    s = s.reindex(periods_dt)
    ts_dict[(prov,amp,tam,var)] = s

# For tambons that are missing entirely for some variables, create empty series
# find all tambon keys
tambon_keys = monthly_clean[['province','amphoe','tambon']].drop_duplicates().to_records(index=False)
all_vars = monthly_clean['variable'].unique().tolist()
for prov, amp, tam in tambon_keys:
    for var in all_vars:
        key = (prov,amp,tam,var)
        if key not in ts_dict:
            ts_dict[key] = pd.Series(index=periods_dt, data=[np.nan]*len(periods_dt))

# Helper: climatology mean for same month across other years
def climatology_fill(s):
    # s: pd.Series indexed by Timestamp monthly start
    clim = s.groupby(s.index.month).mean()
    filled = s.copy()
    na_mask = s.isna()
    for start, end in _find_na_ranges(s):
        months_gap = (end.year - start.year) * 12 + (end.month - start.month) + 1
        if months_gap == 0:
            continue
        # decide threshold based on variable outside
    return clim

# helper to find consecutive Na ranges
def _find_na_ranges(s):
    ranges = []
    in_na = False
    start = None
    prev = None
    for idx, val in s.items():
        if pd.isna(val):
            if not in_na:
                start = idx
                in_na = True
                prev = idx
            else:
                prev = idx
        else:
            if in_na:
                ranges.append((start, prev))
                in_na = False
    if in_na:
        ranges.append((start, prev))
    return ranges

# Gap-fill rules: NDVI/LST threshold >=2 months => climatology of same month, else linear interp
# SMAP threshold >=1 month => climatology, else linear interp
print("Applying gap-fill rules...")
filled_rows = []
for key, s in ts_dict.items():
    prov, amp, tam, var = key
    series = s.copy()
    # remove tiny index issues
    series.index = pd.to_datetime(series.index)
    # compute Na ranges
    na_ranges = _find_na_ranges(series)
    # apply logic per range
    for (a,b) in na_ranges:
        months_gap = (b.year - a.year) * 12 + (b.month - a.month) + 1
        if var in ('NDVI','LST'):
            threshold = 2
        elif var == 'SMAP':
            threshold = 1
        else:
            threshold = None
        if threshold is None:
            # do not fill RAIN or FIRE
            continue
        if months_gap >= threshold:
            # climatology fill: mean of same month other years (use non-null values)
            clim = series.groupby(series.index.month).mean()
            for dt in pd.date_range(start=a, end=b, freq='MS'):
                val = clim.get(dt.month, np.nan)
                series.loc[dt] = val
        else:
            # linear interpolation for small gaps
            series = series.sort_index().interpolate(method='time', limit_direction='both')
    # after fill, keep as is (rain/fire may still be NaN)
    for dt, val in series.items():
        y = dt.year
        m = dt.month
        filled_rows.append({'year':y,'month':m,'province':prov,'amphoe':amp,'tambon':tam,'variable':var,'value': (float(val) if pd.notna(val) else np.nan)})

cleaned = pd.DataFrame(filled_rows)

# ---- Final step: reorder columns as requested and write output ----
ordered_cols = ['year','month','province','amphoe','tambon','variable','value']
cleaned = cleaned[ordered_cols]

out_file = CLEAN_OUT / f"cleaned_combined_{START_YEAR}_{END_YEAR}.parquet"
cleaned.to_parquet(out_file, index=False)
print("Wrote cleaned combined parquet:", out_file)

# Also write sample CSV for recent month (optional)
sample_out = CLEAN_OUT / "cleaned_combined_sample.csv"
cleaned.head(100).to_csv(sample_out, index=False)
print("Wrote sample CSV:", sample_out)
