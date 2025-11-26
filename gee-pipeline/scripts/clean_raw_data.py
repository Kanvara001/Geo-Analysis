# scripts/clean_raw_data.py
import pandas as pd
import numpy as np
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv("config/template_config.env")

RAW_DIR = Path('outputs/raw_parquet')
CLEAN_DIR = Path('outputs/clean')
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

START_YEAR = int(os.getenv('START_YEAR', 2015))
END_YEAR = int(os.getenv('END_YEAR', 2024))

# helper funcs
def monthly_group(df, date_col='image_date', agg='mean'):
    df[date_col] = pd.to_datetime(df[date_col])
    df['year'] = df[date_col].dt.year
    df['month'] = df[date_col].dt.month
    # groupby province/amphoe/tambon/year/month and aggregate numeric by mean or sum
    group_cols = ['province','amphoe','tambon','year','month']
    if agg == 'sum':
        out = df.groupby(group_cols).sum(numeric_only=True).reset_index()
    else:
        out = df.groupby(group_cols).mean(numeric_only=True).reset_index()
    return out

def load_var_parquets(var):
    # load all parquet files for var across years
    base = RAW_DIR / var
    if not base.exists():
        return pd.DataFrame()
    df_list = []
    for p in base.rglob('*.parquet'):
        try:
            d = pd.read_parquet(p)
            # ensure image_date exists: try to find a column that is date-like
            if 'image_date' not in d.columns:
                # try first column that looks like date
                for c in d.columns:
                    if 'date' in c.lower() or 'time' in c.lower():
                        d.rename(columns={c:'image_date'}, inplace=True)
                        break
            df_list.append(d)
        except Exception as e:
            print("Error reading", p, e)
    if df_list:
        return pd.concat(df_list, ignore_index=True, sort=False)
    else:
        return pd.DataFrame()

def fill_long_gap(series, varname, threshold_months):
    # series: pd.Series indexed by period (year,month) or simple dateindex; for simplicity assume indexed by datetime months
    # We'll perform linear interpolation for small gaps; for long gaps replace with climatology (mean of same month other years)
    s = series.copy()
    # create datetime index from Year-Month if needed
    if not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime([f"{y}-{m}-01" for (y,m) in s.index])
    # find NaNs blocks
    isna = s.isna()
    if not isna.any():
        return s
    # compute climatology per month
    clim = s.groupby(s.index.month).mean()
    # find consecutive NaNs
    na_groups = []
    cur_start = None
    prev = None
    for idx, val in isna.iteritems():
        if val and cur_start is None:
            cur_start = idx
            prev = idx
        elif val:
            prev = idx
        elif not val and cur_start is not None:
            na_groups.append((cur_start, prev))
            cur_start = None
    if cur_start is not None:
        na_groups.append((cur_start, prev))
    # for each group decide
    for (a,b) in na_groups:
        months_gap = (b.year - a.year) * 12 + (b.month - a.month) + 1
        if months_gap >= threshold_months:
            # fill each month by climatology
            for dt in pd.date_range(a, b, freq='MS'):
                s.loc[dt] = clim.loc[dt.month]
        else:
            # small gap: linear interpolate
            s = s.sort_index().interpolate(method='time', limit_direction='both')
    return s

def apply_cleaning(ndvi_df, lst_df, smap_df, rain_df, fire_df):
    # produce monthly aggregated data frames
    ndvi_m = monthly_group(ndvi_df, agg='mean') if not ndvi_df.empty else pd.DataFrame()
    lst_m  = monthly_group(lst_df,  agg='mean') if not lst_df.empty else pd.DataFrame()
    smap_m = monthly_group(smap_df, agg='mean') if not smap_df.empty else pd.DataFrame()
    rain_m = monthly_group(rain_df, agg='sum')  if not rain_df.empty else pd.DataFrame()
    fire_m = monthly_group(fire_df, agg='sum')  if not fire_df.empty else pd.DataFrame()

    # create full index of tambon x months
    # get unique tambons from any df
    tambon_keys = None
    for df in [ndvi_m, lst_m, smap_m, rain_m, fire_m]:
        if df is not None and not df.empty:
            tambon_keys = df[['province','amphoe','tambon']].drop_duplicates()
            break
    if tambon_keys is None:
        return None

    # create date index months from start to end
    periods = pd.date_range(start=f"{START_YEAR}-01-01", end=f"{END_YEAR}-12-01", freq='MS')
    out_rows = []

    for _, row in tambon_keys.iterrows():
        prov, amp, tam = row['province'], row['amphoe'], row['tambon']
        # build monthly series for each var
        idx = pd.MultiIndex.from_product([[prov], [amp], [tam], periods], names=['province','amphoe','tambon','period'])
        base = pd.DataFrame(index=idx).reset_index()
        base['year'] = base['period'].dt.year
        base['month'] = base['period'].dt.month

        # helper to extract value series for variable df (which has year/month)
        def series_from_monthly(df_m, col_name):
            if df_m is None or df_m.empty:
                return pd.Series(index=periods, data=np.nan)
            dff = df_m[(df_m['province']==prov) & (df_m['amphoe']==amp) & (df_m['tambon']==tam)]
            s = pd.Series(index=periods, data=np.nan)
            for _, r in dff.iterrows():
                dt = pd.to_datetime(f"{int(r['year'])}-{int(r['month'])}-01")
                # select numeric columns except grouping
                # try to find first numeric column that is not year/month/province...
                numeric_cols = [c for c in r.index if c not in ['province','amphoe','tambon','year','month'] and pd.api.types.is_number(r[c])]
                if len(numeric_cols)>0:
                    s.loc[dt] = r[numeric_cols[0]]
            return s

        s_ndvi = series_from_monthly(ndvi_m, 'ndvi')
        s_lst  = series_from_monthly(lst_m,  'lst')
        s_smap = series_from_monthly(smap_m, 'sm')
        s_rain = series_from_monthly(rain_m, 'rain')
        s_fire = series_from_monthly(fire_m, 'fire')

        # apply gap rules
        s_ndvi_f = fill_long_gap(s_ndvi, 'NDVI', threshold_months=2)
        s_lst_f  = fill_long_gap(s_lst,  'LST',  threshold_months=2)
        s_smap_f = fill_long_gap(s_smap, 'SMAP', threshold_months=1)
        # rain/fire left as-is (no fill)
        # Now for each period create rows per variable
        for dt in periods:
            y,m = dt.year, dt.month
            out_rows.append({'province':prov,'amphoe':amp,'tambon':tam,'variable':'NDVI','value': s_ndvi_f.loc[dt], 'year':y,'month':m})
            out_rows.append({'province':prov,'amphoe':amp,'tambon':tam,'variable':'LST','value': s_lst_f.loc[dt], 'year':y,'month':m})
            out_rows.append({'province':prov,'amphoe':amp,'tambon':tam,'variable':'SMAP','value': s_smap_f.loc[dt], 'year':y,'month':m})
            out_rows.append({'province':prov,'amphoe':amp,'tambon':tam,'variable':'RAIN','value': s_rain.loc[dt] if dt in s_rain.index else np.nan, 'year':y,'month':m})
            out_rows.append({'province':prov,'amphoe':amp,'tambon':tam,'variable':'FIRE','value': s_fire.loc[dt] if dt in s_fire.index else 0, 'year':y,'month':m})

    outdf = pd.DataFrame(out_rows)

    # --- รีเรียงคอลัมน์ตามที่ต้องการ ---
    ordered_cols = [
        'year',
        'month',
        'province',
        'amphoe',
        'tambon',
        'variable',
        'value'
    ]
    outdf = outdf[ordered_cols]
    
    return outdf


if __name__ == "__main__":
    ndvi_df = load_var_parquets('NDVI')
    lst_df  = load_var_parquets('LST')
    smap_df = load_var_parquets('SMAP')
    rain_df = load_var_parquets('RAIN')
    fire_df = load_var_parquets('FIRE')

    print("Loaded tables sizes:", len(ndvi_df), len(lst_df), len(smap_df), len(rain_df), len(fire_df))

    combined = apply_cleaning(ndvi_df, lst_df, smap_df, rain_df, fire_df)
    if combined is not None:
        outp = CLEAN_DIR / f"cleaned_combined_{START_YEAR}_{END_YEAR}.parquet"
        combined.to_parquet(outp, index=False)
        print("Wrote cleaned combined parquet:", outp)
