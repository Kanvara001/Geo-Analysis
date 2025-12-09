import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, callback
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from diskcache import Cache # Library Caching
import os

# ----------------------------------------------------
# --- I. การตั้งค่า Global Variables และ Caching ---
# ----------------------------------------------------

# ตั้งค่า Disk Cache สำหรับ Memoization (ประสิทธิภาพ)
CACHE_DIR = os.path.join(os.getcwd(), "cache_directory")
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
cache = Cache(CACHE_DIR)
memoize = cache.memoize # สร้างฟังก์ชันแคช

# Path File
file_path_xlsx = r'C:\Users\NBODT\my_dash_app\data\df_merged_subdistrict.xlsx' 
file_path_shp = r'C:\Users\NBODT\my_dash_app\data\khonkaen_provinces.shp'

# Global Data Structures
df = pd.DataFrame()
gdf = gpd.GeoDataFrame()
all_dates = []
slider_marks = {}
min_date_index = 0
max_date_index = 0
analysis_vars = ['LST', 'SoilMoisture', 'precipitation', 'FireCount', 'NDVI']
all_provinces = []
all_districts = {}

# --- ฟังก์ชันสำหรับกำหนดสีตามตัวแปร ---
def get_color_scale(variable):
    """คืนค่า Plotly Color Scale ที่เหมาะสมกับตัวแปรที่เลือก"""
    if variable == 'NDVI':
        # เขียวเข้ม(ดี) -> เหลือง(ไม่ดี)
        return 'Viridis_r' 
    if variable == 'LST':
        # ร้อน (แดงเข้ม) -> เย็น (ฟ้า)
        return 'Inferno' 
    if variable == 'FireCount':
        # ไฟป่า (แดงเข้ม)
        return 'YlOrRd'
    if variable == 'SoilMoisture':
        # ชื้น (น้ำเงินเข้ม) -> แห้ง (เหลือง)
        return 'dense'
    if variable == 'precipitation':
        # ฝนมาก (น้ำเงินเข้ม)
        return 'Oceans'
    return 'Plasma'

# --- การโหลดและเตรียมข้อมูล (รันครั้งเดียว) ---
try:
    df = pd.read_excel(file_path_xlsx)
    df.columns = df.columns.str.replace(' ', '_')
    df['year_month_dt'] = pd.to_datetime(df['year_month'], format='%Y-%m')
    df['year_month_label'] = df['year_month_dt'].dt.strftime('%Y-%m')
    
    # เตรียม Time Slider
    all_dates = sorted(df['year_month_label'].unique())
    step = 12 # แสดง mark ทุก 12 เดือน (1 ปี)
    
    # ปรับ Style ของ Slider Mark ให้เข้ากับ SLATE Theme
    slider_marks = {
        i: {'label': date if i % step == 0 or i == len(all_dates) - 1 else '', 
            'style': {'transform': 'rotate(90deg)', 'font-size': '10px', 'white-space': 'nowrap', 'color': '#ABB2B9'}} 
        for i, date in enumerate(all_dates)
    }
    min_date_index = 0
    max_date_index = len(all_dates) - 1
    
    # เตรียม Filter
    all_provinces = sorted(df['Province'].unique())
    for province in all_provinces:
        all_districts[province] = sorted(df[df['Province'] == province]['District'].unique())

    print("✅ โหลดและเตรียมข้อมูล Excel สำเร็จ")
except Exception as e:
    print(f"❌ Error ในการโหลด Excel: {e}")

try:
    gdf = gpd.read_file(file_path_shp)
    if 'Subdistric' in gdf.columns:
        gdf = gdf.rename(columns={'Subdistric': 'Subdistrict'})
        
    # คำนวณจุดศูนย์กลาง (Centroid) ของแต่ละตำบลสำหรับ Marker Map
    gdf['Lat'] = gdf.geometry.centroid.y
    gdf['Lon'] = gdf.geometry.centroid.x
    
    print("✅ โหลดและเตรียม Shapefile สำเร็จ")
except Exception as e:
    print(f"❌ Error ในการโหลด Shapefile: {e}")


# ----------------------------------------------------
# --- II. การเริ่มต้น Dash App และ Layout (ใช้ SLATE Theme) ---
# ----------------------------------------------------

# ใช้ SLATE Theme เพื่อให้มีพื้นหลังสีดำ/เทา
app = Dash(__name__, external_stylesheets=[dbc.themes.SLATE], title="Geo-Analysis Dashboard")

# Layout สำหรับส่วนควบคุม
controls = dbc.Card(
    [
        html.H4("⚙️ ตัวกรองและระดับพื้นที่", className="card-title text-center text-info"),
        html.Hr(),
        
        # 3.1 เลือกตัวแปร (Variable Selector)
        html.Label("ตัวแปรที่ต้องการวิเคราะห์:", className="mt-3 text-light"),
        dcc.Dropdown(
            id='variable-selector',
            options=[{'label': col, 'value': col} for col in analysis_vars],
            value='NDVI', 
            clearable=False,
            className="mb-3 text-dark",
        ),
        
        html.Hr(),

        # 3.2 เลือก Province
        html.Label("กรองตามจังหวัด:", className="mt-3 text-light"),
        dcc.Dropdown(
            id='province-selector',
            options=[{'label': p, 'value': p} for p in all_provinces],
            value=None, 
            placeholder="-- เลือกทุกจังหวัด --",
            className="mb-3 text-dark",
        ),

        # 3.3 เลือก District (Dependent on Province)
        html.Label("กรองตามอำเภอ:", className="mt-3 text-light"),
        dcc.Dropdown(
            id='district-selector',
            options=[],
            value=None,
            placeholder="-- เลือกทุกอำเภอ --",
            className="mb-3 text-dark",
        ),
        
        # 3.4 ระดับการแสดงผล (Subdistrict/District/Heatmap)
        html.Label("รูปแบบการแสดงผลบนแผนที่:", className="mt-3 text-light"),
        dcc.RadioItems(
            id='level-selector',
            options=[
                {'label': ' ตำบล (Choropleth)', 'value': 'Subdistrict'},
                {'label': ' อำเภอ (Choropleth)', 'value': 'District'},
                {'label': ' Heatmap (Grid-like)', 'value': 'Heatmap'} 
            ],
            value='Subdistrict',
            inline=False,
            className="d-flex flex-column mb-3 p-2 bg-secondary rounded"
        ),
    ],
    body=True,
    className="h-100 shadow-lg"
)


app.layout = dbc.Container([
    # Header
    dbc.Row(dbc.Col(html.H1("🛰️ Geo-Analysis Dashboard", 
                           className="text-center my-4 text-info"))), 

    dbc.Row([
        # Column 1: Controls (30%)
        dbc.Col(controls, md=3, className="mb-4"),
        
        # Column 2: Main Map (70%)
        dbc.Col(
            dbc.Card([
                dbc.CardHeader(html.H4(id='map-title-display', className="text-center text-warning")),
                dbc.CardBody([
                    # Time Slider 
                    html.Div([
                        html.Label("⏳ เลือกช่วงเวลา (ปี-เดือน):", className="fw-bold mb-2 text-light"),
                        dcc.RangeSlider(
                            id='time-slider',
                            min=min_date_index,
                            max=max_date_index,
                            step=None,
                            value=[min_date_index, max_date_index],
                            marks=slider_marks,
                            allowCross=False,
                            className='mb-4',
                            tooltip={"placement": "bottom", "always_visible": False, "style": {"font-size": "10px"}}
                        ),
                    ], className="mb-4 p-2 border border-info rounded-3 bg-dark"),

                    # Graph Map
                    dcc.Graph(id='main-map', config={'displayModeBar': True}, style={'height': '70vh'}),
                ])
            ], className="h-100 shadow-lg"),
            md=9,
            className="mb-4"
        ),
    ], className="g-4"), 

    # Time Series Chart & Summary Row
    dbc.Row(dbc.Col(html.Hr(className="my-4"))), 
    dbc.Row(dbc.Col(html.H2("📈 แนวโน้มและสรุปข้อมูล (พื้นที่ที่เลือก)", className="text-center mb-3 text-light"))),
    dbc.Row([
        # Time Series Graph
        dbc.Col(
            dbc.Card(dbc.CardBody(dcc.Graph(id='time-series-chart', config={'displayModeBar': True}))),
            md=8 
        ),
        # Summary Table (Min/Max/Mean)
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardHeader(html.H5("📊 สรุป Min/Max/Mean")),
                    dbc.CardBody(html.Div(id='summary-table'))
                ],
                className="shadow-lg mb-4 h-100"
            ),
            md=4 
        )
    ], className="g-4"),

], fluid=True, className="py-3") 


# ----------------------------------------------------
# --- III. Callback (Interactive & Performance) ---
# ----------------------------------------------------

# 4.1 Callback สำหรับอัปเดต Dropdown อำเภอ (District)
@callback(
    Output('district-selector', 'options'),
    Output('district-selector', 'value'),
    [Input('province-selector', 'value')]
)
def set_district_options(selected_province):
    if selected_province and all_districts:
        options = [{'label': d, 'value': d} for d in all_districts.get(selected_province, [])]
        return options, None 
    return [], None

# 4.2 ฟังก์ชันคำนวณข้อมูลหลักที่ถูกแคช (เพื่อประสิทธิภาพ)
@memoize()
def compute_data_for_map(selected_variable, start_date, end_date, sel_prov, sel_dist, sel_level):
    """ฟังก์ชันคำนวณข้อมูลหลักที่ถูกแคช"""
    df_filtered = df[(df['year_month_dt'] >= start_date) & (df['year_month_dt'] <= end_date)].copy()
    
    # กรองตามภูมิศาสตร์
    if sel_prov:
        df_filtered = df_filtered[df_filtered['Province'] == sel_prov]
    if sel_dist:
        df_filtered = df_filtered[df_filtered['District'] == sel_dist]

    # กำหนดระดับการ Merge/Groupby
    if sel_level == 'Heatmap' or sel_level == 'Subdistrict':
        merge_cols = ['Province', 'District', 'Subdistrict']
    elif sel_level == 'District':
        merge_cols = ['Province', 'District']
    else:
         merge_cols = ['Province', 'District', 'Subdistrict'] # Fallback
    
    # Groupby
    df_map = df_filtered.groupby(merge_cols)[selected_variable].mean().reset_index()
    
    # Merge เฉพาะคอลัมน์ที่จำเป็นสำหรับ Heatmap/Choropleth
    if sel_level == 'Heatmap':
        # สำหรับ Heatmap เราต้อง Merge Lat/Lon ด้วย
        merged_gdf = gdf[['Province', 'District', 'Subdistrict', 'Lat', 'Lon', 'geometry']].merge(
            df_map, on=merge_cols, how='left')
    else:
        # สำหรับ Choropleth
        merged_gdf = gdf.merge(df_map, on=merge_cols, how='left')
    
    # ***แก้ไข ValueError: ส่งค่า 4 ค่า (merged_gdf, df_filtered, merge_cols, df_map) ออกมา***
    return merged_gdf, df_filtered, merge_cols, df_map 

# 4.3 Callback หลัก: อัปเดตแผนที่และ Time Series
@callback(
    [Output('main-map', 'figure'),
     Output('time-series-chart', 'figure'),
     Output('map-title-display', 'children'),
     Output('summary-table', 'children')],
    [Input('variable-selector', 'value'),
     Input('time-slider', 'value'),
     Input('province-selector', 'value'),
     Input('district-selector', 'value'),
     Input('level-selector', 'value')]
)
def update_dashboard(selected_variable, time_range_index, sel_prov, sel_dist, sel_level):
    if df.empty or gdf.empty:
        return {}, {}, "Error: ข้อมูลไม่พร้อม", ""
    
    # 1. เตรียมช่วงเวลา (สำหรับ Key Caching)
    start_date_str = all_dates[time_range_index[0]]
    end_date_str = all_dates[time_range_index[1]]
    start_date = pd.to_datetime(start_date_str, format='%Y-%m')
    end_date = pd.to_datetime(end_date_str, format='%Y-%m')
    
    # 2. คำนวณข้อมูลโดยเรียกจากฟังก์ชันที่ถูกแคช
    merged_gdf, df_filtered, merge_cols, df_map = compute_data_for_map(
        selected_variable, start_date, end_date, sel_prov, sel_dist, sel_level)

    # 3. กำหนด Title
    title_location = "ทุกพื้นที่"
    if sel_prov: title_location = sel_prov
    if sel_dist: title_location = f"{sel_dist}, {sel_prov}"
    
    # --- 4. สร้าง Map Figure ---
    if sel_level in ['Subdistrict', 'District']:
        # Choropleth Map (แผนที่ระบายสีตามขอบเขต)
        fig_map = px.choropleth_mapbox(
            merged_gdf, 
            geojson=merged_gdf.geometry.__geo_interface__, 
            locations=merged_gdf.index, 
            color=selected_variable, 
            color_continuous_scale=get_color_scale(selected_variable),
            mapbox_style="carto-positron", 
            zoom=6.5 if sel_prov is None else 8, 
            center={"lat": 16.1, "lon": 102.8}, 
            opacity=0.8,
            labels={selected_variable: selected_variable},
            hover_name=sel_level, 
            title=None
        )
        fig_map.update_traces(marker_line_width=0.1, marker_opacity=0.7) 
    
    elif sel_level == 'Heatmap':
        # Heatmap / Bubble Map (จำลอง Grid-like)
        fig_map = px.scatter_mapbox(
            merged_gdf.dropna(subset=['Lat', 'Lon', selected_variable]), 
            lat='Lat', 
            lon='Lon', 
            color=selected_variable, 
            size=selected_variable, 
            size_max=15, 
            color_continuous_scale=get_color_scale(selected_variable), 
            mapbox_style="carto-positron",
            zoom=6.5 if sel_prov is None else 8, 
            center={"lat": 16.1, "lon": 102.8}, 
            opacity=0.7,
            labels={selected_variable: selected_variable},
            hover_name='Subdistrict'
        )
    
    else: # Fallback
        fig_map = go.Figure()

    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, uirevision='map-layout') 
    
    # --- 5. สร้าง Time Series Figure ---
    df_ts_all = df_filtered.groupby('year_month_dt')[selected_variable].mean().reset_index()
    
    fig_ts = px.line(df_ts_all, x='year_month_dt', y=selected_variable)
    fig_ts.update_layout(
        title_text=f'แนวโน้มรายเดือนของ {selected_variable}',
        xaxis_title='เดือน/ปี',
        yaxis_title=selected_variable,
        template='plotly_dark'
    )
    
    # เน้นช่วงเวลาที่เลือก
    fig_ts.add_vrect(x0=start_date, x1=end_date, fillcolor="#FFD700", opacity=0.3, line_width=0)
    
    # --- 6. สร้างตารางสรุป Min/Max/Mean ---
    summary_table = html.P("ไม่พบข้อมูลในช่วงเวลาที่เลือก", className="text-warning")
    if not df_map.empty and not merged_gdf[selected_variable].isnull().all(): 
        valid_data = merged_gdf.dropna(subset=[selected_variable])
        df_summary = valid_data[selected_variable].agg(['min', 'max', 'mean']).to_frame().T.round(3)
        
        idx_min = valid_data[selected_variable].idxmin()
        idx_max = valid_data[selected_variable].idxmax()
        
        min_loc = valid_data.loc[idx_min, merge_cols].to_dict()
        max_loc = valid_data.loc[idx_max, merge_cols].to_dict()

        summary_table = dbc.Table(
            [
                html.Thead(html.Tr([html.Th("สถิติ", className="text-info"), html.Th("ค่า", className="text-info"), html.Th("พื้นที่ (Min/Max)", className="text-info")]), style={'background-color': '#343a40'}),
                html.Tbody([
                    html.Tr([html.Td("Min"), html.Td(df_summary['min'].iloc[0]), html.Td(', '.join(min_loc.values()))]),
                    html.Tr([html.Td("Max"), html.Td(df_summary['max'].iloc[0]), html.Td(', '.join(max_loc.values()))]),
                    html.Tr([html.Td("Mean"), html.Td(df_summary['mean'].iloc[0]), html.Td("-")]),
                ])
            ],
            bordered=True,
            hover=True,
            responsive=True,
            striped=True,
            className="mt-3 table-dark"
        )
    
    map_title = html.Span([
        html.Span(f"การกระจายของ {selected_variable} ({sel_level})"),
        html.Br(),
        html.Small(f"พื้นที่: {title_location} | ช่วงเวลา: {start_date_str} ถึง {end_date_str}", className="text-muted")
    ])
    
    return fig_map, fig_ts, map_title, summary_table


# --- 6. รัน App ---
if __name__ == '__main__':
    cache.clear() 
    app.run(debug=True, port=8050)