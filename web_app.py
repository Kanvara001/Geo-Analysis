from flask import Flask, render_template
import dash
from dash import dcc, html, Input, Output, State, ctx, no_update
import dash_bootstrap_components as dbc
import geopandas as gpd
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# --- 1. CONFIG & DATA LOADING ---

server = Flask(__name__)

external_stylesheets = [
    dbc.themes.FLATLY,
]

app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets)
app.title = "GeoViztion: Drought Monitoring"

print("Loading and Cleaning Data...")

# Load Main Data
df_main = pd.read_parquet('data/merged_dataset_FILLED.parquet')
df_main['subdistrict'] = df_main['subdistrict'].astype(str).str.strip()
df_main['district'] = df_main['district'].astype(str).str.strip()
if 'province' not in df_main.columns:
    df_main['province'] = 'Khon Kaen'
else:
    df_main['province'] = df_main['province'].astype(str).str.strip()

df_main['unique_id'] = df_main['district'] + "_" + df_main['subdistrict']

# Create Date Column (สำคัญสำหรับรายเดือน)
df_main['day'] = 1
df_main['date'] = pd.to_datetime(df_main[['year', 'month', 'day']])

# Load DTW Data
df_dtw = pd.read_parquet('data/dtw_results.parquet')
df_dtw['subdistrict'] = df_dtw['subdistrict'].astype(str).str.strip()
df_dtw['district'] = df_dtw['district'].astype(str).str.strip()
df_dtw['unique_id'] = df_dtw['district'] + "_" + df_dtw['subdistrict']
if 'province' not in df_dtw.columns and 'province' in df_main.columns:
     prov_map = df_main[['district', 'province']].drop_duplicates().set_index('district')['province'].to_dict()
     df_dtw['province'] = df_dtw['district'].map(prov_map)
# สร้าง date ให้ DTW ด้วย (เผื่อใช้)
df_dtw['day'] = 1
df_dtw['date'] = pd.to_datetime(df_dtw[['year', 'month', 'day']]) if 'month' in df_dtw.columns else pd.to_datetime(df_dtw['year'].astype(str) + '-01-01')


# Load Shapefile & Dissolve for Borders
# 1. โหลดไฟล์และแปลง CRS
gdf = gpd.read_file('shapefile/khonkaen_provinces.shp', encoding='utf-8')
gdf = gdf.to_crs(epsg=4326)

# 2. [สำคัญ] เปลี่ยนชื่อคอลัมน์ให้เป็นมาตรฐาน 'ก่อน' จะทำอย่างอื่น
# เช็คชื่อคอลัมน์จริงในไฟล์คุณ ถ้า Error อีก ให้ดู print(gdf.columns) แล้วแก้ key ใน col_map
col_map = {
    'Subdistric': 'subdistrict', 
    'District': 'district', 
    'Province': 'province',
    'PROVINCE': 'province',   # เผื่อไฟล์เป็นตัวพิมพ์ใหญ่หมด
    'Prov_Nam_T': 'province'  # เผื่อเป็นชื่อไทย
}
gdf.rename(columns={k:v for k,v in col_map.items() if k in gdf.columns}, inplace=True)

# Clean ข้อมูลชื่อ (ลบช่องว่างหน้าหลัง)
if 'subdistrict' in gdf.columns: gdf['subdistrict'] = gdf['subdistrict'].astype(str).str.strip()
if 'district' in gdf.columns: gdf['district'] = gdf['district'].astype(str).str.strip()
if 'province' not in gdf.columns: 
    # ถ้าเปลี่ยนชื่อแล้วยังไม่มี column province ให้สร้างขึ้นมา (กรณีไฟล์มีจังหวัดเดียว)
    gdf['province'] = 'Khon Kaen' 

gdf['unique_id'] = gdf['district'] + "_" + gdf['subdistrict']

# 3. Simplify ขอบเขตตำบลลดความละเอียดลง (เพื่อให้โหลดเร็ว)
gdf['geometry'] = gdf['geometry'].simplify(tolerance=0.001, preserve_topology=True)

# 4. สร้างเส้นขอบจังหวัดหนา (Dissolve)
# ต้องทำหลังจากที่มี column 'province' แล้วเท่านั้น
gdf_temp_dissolve = gdf[['province', 'geometry']].copy()
gdf_temp_dissolve['geometry'] = gdf_temp_dissolve['geometry'].buffer(0.001) # ขยายอุดรูรั่ว
gdf_prov = gdf_temp_dissolve.dissolve(by='province')
gdf_prov['geometry'] = gdf_prov['geometry'].simplify(0.001) # เกลาให้เรียบ

geojson_province = json.loads(gdf_prov.to_json())

# 5. สร้าง GeoJSON ระดับตำบล
geojson = json.loads(gdf.to_json())
feature_key = 'properties.unique_id'

print("Data Loaded.")

# --- 2. CONSTANTS & TIME UTILS ---
VARIABLE_INFO = {
    'NDVI': {'label': 'ดัชนีพืชพรรณ (NDVI)', 'color': 'Greens', 'line_color': '#16a34a', 'dtw_name': 'ndvi', 'unit': ''},
    'LST': {'label': 'อุณหภูมิผิวดิน (LST)', 'color': 'YlOrRd', 'line_color': '#ea580c', 'dtw_name': 'lst', 'unit': '°C'},
    'SOILMOISTURE': {'label': 'ความชื้นในดิน', 'color': 'Blues', 'line_color': '#2563eb', 'dtw_name': 'soilmoisture', 'unit': '%'},
    'RAINFALL': {'label': 'ปริมาณฝน', 'color': 'Blues', 'line_color': '#0891b2', 'dtw_name': 'rainfall', 'unit': 'mm'},
    'FIRECOUNT': {'label': 'จำนวนจุดไฟ', 'color': 'Reds', 'line_color': '#dc2626', 'dtw_name': 'firecount', 'unit': 'จุด'}
}

all_provinces = sorted(df_main['province'].unique())

# --- [ส่วนใหม่] เตรียมข้อมูลสำหรับ Slider รายเดือน ---
# สร้างลิสต์เดือนทั้งหมดที่มีในข้อมูล
min_date = df_main['date'].min()
max_date = df_main['date'].max()
all_months_pd = pd.date_range(start=min_date, end=max_date, freq='MS') # MS = Month Start
ALL_MONTHS_LABELS = [d.strftime('%b %Y') for d in all_months_pd] # e.g., "Jan 2015"
ALL_MONTHS_VALUES = list(range(len(all_months_pd))) # 0, 1, 2, ...
MONTH_MAP = dict(zip(ALL_MONTHS_VALUES, all_months_pd)) # {0: Timestamp('2015-01-01'), ...}

# สร้าง Marks ให้โชว์แค่ปี (ทุกๆ 12 เดือน) เพื่อไม่ให้รก
SLIDER_MARKS = {i: {'label': d.strftime('%Y'), 'style': {'fontSize': '10px'}} 
                for i, d in zip(ALL_MONTHS_VALUES, all_months_pd) 
                if d.month == 1} # โชว์เฉพาะเดือนมกราคมของแต่ละปี

# --- 3. COMPONENTS ---

def create_control_panel():
    return html.Div([
        # Header
        dbc.Row([
            dbc.Col([
                html.A([
                    html.Img(src="assets/images/logo.png", 
                             style={'height':'65px', 'width':'65px', 'borderRadius':'50%', 'border':'2px solid white', 'padding':'2px'}, 
                             className="me-3 shadow-sm"),
                    html.Div([
                        html.H4("GeoViztion", className="mb-0 fw-bold", 
                                style={'fontSize': '28px', 'letterSpacing': '1px'}), 
                        html.Small("Drought Monitoring System", style={'opacity':'0.8', 'fontWeight': '300'})
                    ], className="d-flex flex-column")
                ], href="/", className="d-flex align-items-center text-decoration-none text-white")
            ], width=6),
            dbc.Col([
                dbc.Button("Dashboard", href="/", color="light", className="me-3 fw-bold rounded-pill px-4 shadow-sm"),
                dbc.Button("About Us", href="/about", external_link=True, color="outline-light", className="fw-bold rounded-pill px-4")
            ], width=6, className="d-flex justify-content-end align-items-center")
        ], className="px-5 py-4 mx-0 align-items-center shadow", 
           style={'background': 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)', 'color': 'white', 'borderBottom': '1px solid rgba(255,255,255,0.1)'}),

        # Filter Bar
        dbc.Row([
            # Variable
            dbc.Col([
                html.Label("Variable", className="small fw-bold text-secondary mb-1"),
                dcc.Dropdown(
                    id='variable-dropdown',
                    options=[{'label': v['label'], 'value': k} for k, v in VARIABLE_INFO.items()],
                    value='NDVI', clearable=False, style={'fontSize': '0.9rem'}
                )
            ], width=12, sm=6, lg=2, className="mb-2"),

            # Province
            dbc.Col([
                html.Label("Province", className="small fw-bold text-secondary mb-1"),
                dcc.Dropdown(
                    id='province-dropdown',
                    options=[{'label': p, 'value': p} for p in all_provinces],
                    value=[], multi=True, placeholder="All Provinces", style={'fontSize': '0.9rem'}
                )
            ], width=12, sm=6, lg=2, className="mb-2"),

            # District
            dbc.Col([
                html.Label("District", className="small fw-bold text-secondary mb-1"),
                dcc.Dropdown(
                    id='district-dropdown',
                    options=[], value=[], multi=True, placeholder="All Districts", style={'fontSize': '0.9rem'}
                )
            ], width=12, sm=6, lg=2, className="mb-2"),

            # Subdistrict
            dbc.Col([
                html.Label("Subdistrict", className="small fw-bold text-secondary mb-1"),
                dcc.Dropdown(
                    id='subdistrict-dropdown',
                    options=[], value=[], multi=True, placeholder="All Subdistricts", style={'fontSize': '0.9rem'}
                )
            ], width=12, sm=6, lg=2, className="mb-2"),

            # Time Slider (Monthly)
            dbc.Col([
                html.Div([
                    html.Label("Time Period", className="small fw-bold text-secondary mb-0"),
                    # แสดงข้อความช่วงเวลาที่เลือก
                    html.Span(id='date-display', className="small fw-bold text-primary ms-2") 
                ], className="d-flex align-items-center mb-1"),
                
                dcc.RangeSlider(
                    id='month-slider', # เปลี่ยนชื่อ ID
                    min=ALL_MONTHS_VALUES[0], 
                    max=ALL_MONTHS_VALUES[-1], 
                    step=1, # ขยับทีละ 1 เดือน
                    value=[ALL_MONTHS_VALUES[0], ALL_MONTHS_VALUES[-1]], # Default ทั้งหมด
                    marks=SLIDER_MARKS,
                    tooltip={"placement": "bottom", "always_visible": False},
                    updatemode='mouseup'
                )
            ], width=12, lg=4, className="d-flex flex-column justify-content-center pt-2")
            
        ], className="px-4 py-3 mx-0 border-bottom bg-white shadow-sm align-items-start")
    ])

# --- 4. APP LAYOUT ---
card_style = "bg-white rounded-4 shadow-sm h-100 p-4 border border-light"

app.layout = html.Div([
    dcc.Store(id='filter-store', data={}), 
    
    create_control_panel(),

    dbc.Container([
        # Row 1: Map & Radar
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H5("Spatial Distribution", className="text-dark mb-3 fw-bold border-start border-4 border-primary ps-2"),
                    dcc.Loading(dcc.Graph(id='choropleth-map', style={'height': '450px'}, config={'displayModeBar': False}))
                ], className=card_style)
            ], width=12, lg=6),

            dbc.Col([
                html.Div([
                    html.H5("DTW Anomaly Summary", className="text-dark mb-3 fw-bold border-start border-4 border-primary ps-2"),
                    dcc.Loading(dcc.Graph(id='radar-chart', style={'height': '450px'}, config={'displayModeBar': False}))
                ], className=card_style)
            ], width=12, lg=6),
        ], className="g-4 mb-4"),

        # Row 2: Heatmap & Line Plot
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H5("Spatiotemporal Heatmap", className="text-dark mb-3 fw-bold border-start border-4 border-primary ps-2"),
                    
                    # --- ส่วนที่แก้ไข ---
                    dcc.Loading(
                        html.Div([
                            dcc.Graph(id='heatmap-chart', config={'displayModeBar': False})
                        ], style={'height': '450px', 'overflowY': 'scroll', 'overflowX': 'hidden'})
                    )
                    # -----------------

                ], className=card_style)
            ], width=12, lg=6),
            

            dbc.Col([
                html.Div([
                    html.H5("Temporal Analysis", className="text-dark mb-3 fw-bold border-start border-4 border-primary ps-2"),
                    dcc.Loading(dcc.Graph(id='line-plot', style={'height': '450px'}, config={'displayModeBar': False}))
                ], className=card_style)
            ], width=12, lg=6),
        ], className="g-4")

    ], fluid=True, className="p-4")

], style={'minHeight': '100vh', 'fontFamily': 'Prompt, sans-serif'})

# --- 5. CALLBACKS ---

# 5.0 Update Date Label above Slider
@app.callback(
    Output('date-display', 'children'),
    Input('month-slider', 'value')
)
def update_date_label(value_range):
    start_idx, end_idx = value_range
    start_date = MONTH_MAP[start_idx]
    end_date = MONTH_MAP[end_idx]
    return f"({start_date.strftime('%b %Y')} - {end_date.strftime('%b %Y')})"

# 5.1 Chained Dropdowns
@app.callback(
    Output('district-dropdown', 'options'), Output('district-dropdown', 'value'),
    Input('province-dropdown', 'value')
)
def update_districts(selected_provinces):
    if not selected_provinces: 
        dists = sorted(df_main['district'].unique())
        return [{'label': d, 'value': d} for d in dists], []
    dists = sorted(df_main[df_main['province'].isin(selected_provinces)]['district'].unique())
    return [{'label': d, 'value': d} for d in dists], []

@app.callback(
    Output('subdistrict-dropdown', 'options'), Output('subdistrict-dropdown', 'value'),
    Input('district-dropdown', 'value'), State('province-dropdown', 'value')
)
def update_subdistricts(selected_districts, selected_provinces):
    if not selected_districts:
        if selected_provinces:
             subs = sorted(df_main[df_main['province'].isin(selected_provinces)]['subdistrict'].unique())
        else:
             subs = sorted(df_main['subdistrict'].unique())
        return [{'label': s, 'value': s} for s in subs], []

    subs = sorted(df_main[df_main['district'].isin(selected_districts)]['subdistrict'].unique())
    return [{'label': s, 'value': s} for s in subs], []

# 5.3 Filter Logic Store
@app.callback(
    Output('filter-store', 'data'),
    Input('province-dropdown', 'value'),
    Input('district-dropdown', 'value'),
    Input('subdistrict-dropdown', 'value'),
    Input('choropleth-map', 'clickData')
)
def update_filter_store(provs, dists, subs, map_click):
    trigger = ctx.triggered_id
    if trigger == 'choropleth-map' and map_click:
        unique_id = map_click['points'][0]['customdata'][0]
        return {'type': 'single_click', 'id': unique_id}
    return {
        'type': 'filter',
        'provinces': provs if provs else [],
        'districts': dists if dists else [],
        'subdistricts': subs if subs else []
    }

# 5.4 Main Charts (Monthly Logic - Fixed)
@app.callback(
    [Output('choropleth-map', 'figure'), 
     Output('radar-chart', 'figure'), 
     Output('heatmap-chart', 'figure'),
     Output('line-plot', 'figure')],
    [Input('month-slider', 'value'), 
     Input('variable-dropdown', 'value'),
     Input('filter-store', 'data')]
)
def update_all_charts(month_range, variable, filter_data):
    # --- 1. เตรียมข้อมูลเวลา (Time Prep) ---
    start_idx, end_idx = month_range
    start_date = MONTH_MAP[start_idx]
    end_date = MONTH_MAP[end_idx]
    var_meta = VARIABLE_INFO[variable]

    if start_date == end_date:
        time_text = start_date.strftime('%b %Y')
        map_title_text = f"{var_meta['label']} ({time_text})"
    else:
        time_text = f"{start_date.strftime('%b %Y')} - {end_date.strftime('%b %Y')}"
        map_title_text = f"{var_meta['label']} (Avg: {time_text})"

    # --- 2. Filter เวลา (Time Filter) ---
    df_filtered = df_main[(df_main['date'] >= start_date) & (df_main['date'] <= end_date)]
    df_dtw_filtered = df_dtw[(df_dtw['date'] >= start_date) & (df_dtw['date'] <= end_date)]

    # --- 3. Filter ขอบเขตพื้นที่ (Scope Filter) ---
    # เราต้องสร้าง df_chart_scope (ขอบเขตที่เลือก) ก่อน เพื่อใช้คำนวณหาตัวท็อป
    
    # Default คือเลือกทั้งหมด
    df_chart_scope = df_filtered
    df_dtw_chart_scope = df_dtw_filtered
    scope_title_suffix = "All Areas"
    
    # ตัวแปรสำหรับเก็บข้อมูลตำบลที่จะ Highlight (Target)
    target_subdistrict_info = None 
    selection_mode = "manual" # manual หรือ auto

    if filter_data:
        # 3.1 กรณีคลิกจากแผนที่ (Single Click)
        if filter_data.get('type') == 'single_click':
            target_id = filter_data['id']
            
            # Scope สำหรับ Heatmap ยังคงเป็น "ทั้งหมด" หรือ "จังหวัดของตัวที่คลิก" ก็ได้ 
            # แต่เพื่อให้สอดคล้องกับ UX ทั่วไป ให้ Scope เป็นข้อมูลระดับจังหวัดของตัวที่คลิก เพื่อให้เห็นบริบท
            clicked_row = df_main[df_main['unique_id'] == target_id].iloc[0]
            prov_of_click = clicked_row['province']
            
            df_chart_scope = df_filtered[df_filtered['province'] == prov_of_click]
            df_dtw_chart_scope = df_dtw_filtered[df_dtw_filtered['province'] == prov_of_click]
            scope_title_suffix = f"Context: {prov_of_click}"

            # Set Target
            target_subdistrict_info = {
                'id': target_id,
                'name': clicked_row['subdistrict'],
                'full_name': f"{clicked_row['subdistrict']}, {clicked_row['district']}"
            }
            selection_mode = "manual"

        # 3.2 กรณีเลือกจาก Dropdown (Filter)
        elif filter_data.get('type') == 'filter':
            provs = filter_data.get('provinces', [])
            dists = filter_data.get('districts', [])
            subs = filter_data.get('subdistricts', [])

            # Apply Filter to Scope
            if provs:
                df_chart_scope = df_chart_scope[df_chart_scope['province'].isin(provs)]
                df_dtw_chart_scope = df_dtw_chart_scope[df_dtw_chart_scope['province'].isin(provs)]
                scope_title_suffix = f"{len(provs)} Provinces"
            if dists:
                df_chart_scope = df_chart_scope[df_chart_scope['district'].isin(dists)]
                df_dtw_chart_scope = df_dtw_chart_scope[df_dtw_chart_scope['district'].isin(dists)]
                scope_title_suffix = f"{len(dists)} Districts"
            if subs:
                df_chart_scope = df_chart_scope[df_chart_scope['subdistrict'].isin(subs)]
                df_dtw_chart_scope = df_dtw_chart_scope[df_dtw_chart_scope['subdistrict'].isin(subs)]
                scope_title_suffix = f"{len(subs)} Subdistricts"

                # ถ้าเลือกเจาะจง 1 ตำบลใน Dropdown
                if len(subs) == 1:
                    target_name = subs[0]
                    # หา ID (เอาตัวแรกที่เจอใน scope)
                    if not df_chart_scope.empty:
                        row = df_chart_scope.iloc[0]
                        target_subdistrict_info = {
                            'id': row['unique_id'],
                            'name': target_name,
                            'full_name': f"{target_name}, {row['district']}"
                        }
                        selection_mode = "manual"

    # --- 4. Intelligent Selection Logic (หาตัว Highlight ถ้ายังไม่มี Target) ---
    if target_subdistrict_info is None:
        selection_mode = "auto"
        
        # ใช้ df_dtw_chart_scope ที่เตรียมไว้ด้านบน
        dtw_col = f"dtw_{var_meta['dtw_name']}_z"
        
        if dtw_col in df_dtw_chart_scope.columns and not df_dtw_chart_scope.empty:
            ranking = df_dtw_chart_scope.groupby(['unique_id', 'subdistrict', 'district'])[dtw_col].mean().reset_index()
            
            if not ranking.empty:
                if variable == 'LST': # ยิ่งมากยิ่งแย่ (ร้อน)
                    top_row = ranking.sort_values(by=dtw_col, ascending=False).iloc[0]
                else: # ยิ่งน้อยยิ่งแย่ (แล้ง)
                    top_row = ranking.sort_values(by=dtw_col, ascending=True).iloc[0]
                
                target_subdistrict_info = {
                    'id': top_row['unique_id'],
                    'name': top_row['subdistrict'],
                    'full_name': f"{top_row['subdistrict']}, {top_row['district']}"
                }

    # --- 5. เตรียมข้อมูล Target สำหรับ Radar/Line ---
    df_chart_target = pd.DataFrame()
    df_dtw_chart_target = pd.DataFrame()
    chart_title_suffix = ""

    if target_subdistrict_info:
        t_id = target_subdistrict_info['id']
        # ดึงข้อมูล Target จาก df_filtered (ข้อมูลดิบ) และ df_dtw_filtered
        df_chart_target = df_filtered[df_filtered['unique_id'] == t_id]
        df_dtw_chart_target = df_dtw_filtered[df_dtw_filtered['unique_id'] == t_id]
        
        if selection_mode == "manual":
            chart_title_suffix = f"{target_subdistrict_info['full_name']}"
        else:
            chart_title_suffix = f"Highlight: {target_subdistrict_info['name']} (Critical)"
    else:
        chart_title_suffix = "No Data"


    # ==========================
    # CHART 1: MAP (แก้ไข: ใช้ df_chart_scope แทน df_filtered)
    # ==========================

    # [NEW - ถูกต้อง] ใช้ df_chart_scope ที่ผ่านการกรอง Province/District มาแล้ว
    map_agg = df_chart_scope.groupby(['unique_id', 'subdistrict', 'district', 'province'])[variable].mean().reset_index()
    
    map_agg['time_label'] = time_text
    
    fig_map = px.choropleth_mapbox(
        map_agg, geojson=geojson, locations='unique_id', featureidkey=feature_key,
        color=variable, color_continuous_scale=var_meta['color'],
        range_color=(df_main[variable].quantile(0.05), df_main[variable].quantile(0.95)),
        mapbox_style="open-street-map", 
        center={"lat": 16.44, "lon": 102.83}, zoom=7.5, opacity=0.5,
        custom_data=['subdistrict', 'province', 'district', 'time_label'],
        labels={variable: var_meta['unit']}
    )
    fig_map.update_layout(
        margin={"r":0,"t":40,"l":0,"b":0},
        paper_bgcolor='rgba(0,0,0,0)',
        title={'text': map_title_text, 'y': 0.95, 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top', 'font': {'size': 16, 'family': 'Prompt, sans-serif', 'color': '#334155'}},
        hoverlabel=dict(bgcolor="white", bordercolor="#cbd5e1", font_size=14, font_family="'Prompt', sans-serif", align="left")
    )
    fig_map.update_traces(
        hovertemplate=(
            f"<b style='color: #334155; font-size: 14px; font-family: sans-serif;'>{var_meta['label']}</b><br>" +
            "<span style='font-size: 6px;'> </span><br>" + 
            f"<b style='color: {var_meta['line_color']}; font-size: 28px; font-family: Arial;'>%{{z:.2f}}</b> " +
            f"<b style='color: #475569; font-size: 16px;'>{var_meta['unit']}</b><br>" +
            "<span style='color: #cbd5e1; font-weight: bold;'>━━━━━━━━━━━━━━━</span><br>" +
            "<span style='font-size: 13px; font-family: sans-serif; color: #1e293b; line-height: 1.8;'>" +
            "<span style='color: #64748b;'>Province:</span> <b style='font-size: 14px;'>%{customdata[1]}</b><br>" +
            "<span style='color: #64748b;'>District:</span> <b>%{customdata[2]}</b><br>" +
            "<span style='color: #64748b;'>Subdistrict:</span> <b>%{customdata[0]}</b>" +
            "</span><br>" +
            "<br>" +
            "<span style='color: #64748b; font-size: 12px;'>📅 <b>%{customdata[3]}</b></span>" +
            "<extra></extra>"
        )
    )
    fig_map.update_layout(mapbox={'layers': [{"sourcetype": "geojson", "source": geojson_province, "type": "line", "color": "#000000", "line": {"width": 2.5}}]})

    # ==========================
    # CHART 2: RADAR (ใช้ df_dtw_chart_target)
    # ==========================
    fig_radar = go.Figure()
    if not df_dtw_chart_target.empty:
        radar_values, radar_cats = [], []
        for k, v in VARIABLE_INFO.items():
            col_name = f"dtw_{v['dtw_name']}_z"
            if col_name in df_dtw_chart_target.columns:
                radar_values.append(df_dtw_chart_target[col_name].mean())
                radar_cats.append(v['label'])
        
        if radar_values:
            radar_values += [radar_values[0]]; radar_cats += [radar_cats[0]]
            # สีแดงถ้า Auto, สีน้ำเงินถ้า Manual
            line_color = '#3b82f6' if selection_mode == 'manual' else '#ef4444'
            fill_color = 'rgba(59, 130, 246, 0.2)' if selection_mode == 'manual' else 'rgba(239, 68, 68, 0.2)'
            
            fig_radar.add_trace(go.Scatterpolar(
                r=radar_values, theta=radar_cats, fill='toself', 
                line_color=line_color, fillcolor=fill_color
            ))
            
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True), angularaxis=dict(visible=True)),
        paper_bgcolor='rgba(0,0,0,0)', margin=dict(l=40, r=40, t=30, b=20),
        title=dict(text=f"Anomaly: {chart_title_suffix}", x=0.5, font=dict(size=14))
    )

    # ==========================
    # CHART 3: HEATMAP (แก้ไข: โชว์ทุกตำบล + รายเดือน + Dynamic Height)
    # ==========================
    
    # 1. ใช้ข้อมูลทั้งหมดใน Scope ไม่ต้องตัด Top 25
    df_heat = df_chart_scope.copy()
    
    # เรียงลำดับตำบล (เพื่อให้หาง่าย) หรือเรียงตามความรุนแรงก็ได้
    # กรณีนี้เรียงตามตัวอักษรเพื่อให้หาชื่อตำบลเจอง่ายๆ
    sort_order = sorted(df_heat['subdistrict'].unique(), reverse=True) # reverse=True เพื่อให้ ก-ฮ ไล่จากบนลงล่างในกราฟ

    # 2. คำนวณความสูงของกราฟตามจำนวนตำบล (Dynamic Height)
    # สมมติให้แถวนึงสูง 30px ถ้ามีน้อยกว่า 10 ตำบล ให้สูงขั้นต่ำ 400px
    num_rows = len(sort_order)
    fig_height = max(400, num_rows * 30) 

    title_heat = f"Spatiotemporal Heatmap: {scope_title_suffix}"

    # 3. สร้างกราฟ
    fig_heat = px.density_heatmap(
        df_heat, 
        x='date', 
        y='subdistrict', 
        z=variable,
        color_continuous_scale=var_meta['color'],
        category_orders={'subdistrict': sort_order},
        labels={'date': '', variable: var_meta['unit']},
        nbinsx=len(df_heat['date'].unique()) # บังคับให้แบ่งช่องตามจำนวนเดือนที่มีจริง
    )

    # [ส่วนแก้ไข Heatmap Layout]
    fig_heat.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', 
        plot_bgcolor='rgba(0,0,0,0)',
        
        # [แก้] ปรับ margin ซ้าย-ขวา เป็น 0 เพื่อให้ชิดขอบที่สุด
        margin=dict(t=50, b=20, l=0, r=0), 
        
        title=dict(text=title_heat, x=0, font=dict(size=16)),
        height=fig_height,
        
        # [สำคัญมาก] สั่งให้กราฟขยายเต็มพื้นที่ Container
        autosize=True, 
        
        xaxis=dict(
            dtick="M1",
            tickformat="%b %Y",
            showgrid=False,
            # [เสริม] ให้ Plotly คำนวณพื้นที่ label แกน X อัตโนมัติ
            automargin=True 
        ),
        yaxis=dict(
            dtick=1,
            # [เสริม] ให้ Plotly คำนวณพื้นที่ label แกน Y (ชื่อตำบล) อัตโนมัติ
            automargin=True
        )
    )
    
    # เพิ่มเส้น Grid ให้ดูง่ายขึ้น เหมือนตาราง
    fig_heat.update_traces(xgap=1, ygap=1)

    # ==========================
    # CHART 4: LINE PLOT (ใช้ df_chart_target)
    # ==========================
    fig_line = go.Figure()
    if not df_chart_target.empty:
        line_agg = df_chart_target.groupby('date')[variable].mean().reset_index().sort_values('date')
        fig_line.add_trace(go.Scatter(x=line_agg['date'], y=line_agg[variable], mode='lines+markers', 
                                      name='Actual', line=dict(color=var_meta['line_color'])))

        baseline_col_prefix = f"baseline_{var_meta['dtw_name']}_m"
        baseline_monthly_vals = {}
        for m in range(1, 13):
            col = f"{baseline_col_prefix}{m:02d}"
            if col in df_dtw_chart_target.columns:
                baseline_monthly_vals[m] = df_dtw_chart_target[col].mean()
                
        if baseline_monthly_vals:
            baseline_dates = pd.date_range(start=start_date, end=end_date, freq='MS')
            baseline_values = [baseline_monthly_vals[d.month] for d in baseline_dates]
            fig_line.add_trace(go.Scatter(x=baseline_dates, y=baseline_values, 
                                          mode='lines', name='Baseline', line=dict(color='gray', dash='dash')))

    fig_line.update_layout(
        template='plotly_white', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        title=f"Trend: {chart_title_suffix}", 
        hovermode="x unified", margin=dict(t=30, b=0)
    )

    return fig_map, fig_radar, fig_heat, fig_line

@server.route('/about')
def serve_about():
    return render_template('about.html')

if __name__ == '__main__':
    app.run(debug=True, port=8050)
