# scripts/utils_cleaning.py
import pandas as pd
import numpy as np
# -----------------------------
# IQR Outlier Remove + Interpolate
# -----------------------------
def remove_outlier_iqr_interpolate(df, col):
if col not in df.columns:
return df


q1 = df[col].quantile(0.25)
q3 = df[col].quantile(0.75)
iqr = q3 - q1
lower = q1 - 1.5 * iqr
upper = q3 + 1.5 * iqr


# mark outliers as NaN
df[col] = df[col].mask((df[col] < lower) | (df[col] > upper), np.nan)


# interpolate (linear) both directions
df[col] = df[col].interpolate(method="linear", limit_direction="both")


return df
# -----------------------------
df['rain_mm'] = pd.to_numeric(df['sum'], errors='coerce')
elif 'precipitation' in df.columns:
df['rain_mm'] = pd.to_numeric(df['precipitation'], errors='coerce')
else:
numcols = df.select_dtypes(include='number').columns
df['rain_mm'] = pd.to_numeric(df[numcols[0]], errors='coerce') if len(numcols)>0 else np.nan


df = remove_outlier_iqr_interpolate(df, 'rain_mm')


keep = []
for c in ['province','amphoe','tambon','rain_mm']:
if c in df.columns:
keep.append(c)
return df[keep]




def clean_smap(df):
df = df.copy()
if 'mean' in df.columns:
df['sm'] = pd.to_numeric(df['mean'], errors='coerce')
else:
numcols = df.select_dtypes(include='number').columns
df['sm'] = pd.to_numeric(df[numcols[0]], errors='coerce') if len(numcols)>0 else np.nan


df = remove_outlier_iqr_interpolate(df, 'sm')


keep = []
for c in ['province','amphoe','tambon','sm']:
if c in df.columns:
keep.append(c)
return df[keep]




def clean_fire(df):
df = df.copy()
if 'fire_count' in df.columns:
df['fire_count'] = pd.to_numeric(df['fire_count'], errors='coerce').fillna(0).astype(int)
elif 'count' in df.columns:
df['fire_count'] = pd.to_numeric(df['count'], errors='coerce').fillna(0).astype(int)
else:
numcols = df.select_dtypes(include='number').columns
df['fire_count'] = pd.to_numeric(df[numcols[0]], errors='coerce').fillna(0).astype(int) if len(numcols)>0 else 0


keep = []
for c in ['province','amphoe','tambon','fire_count']:
if c in df.columns:
keep.append(c)
return df[keep]
