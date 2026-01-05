import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime

# ----------------------------------------------------
# 1. UI CONFIGURATION
# ----------------------------------------------------
st.set_page_config(page_title="Spatial Insight DSS", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;800&display=swap');
    
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 2rem !important;
    }
    
    [data-testid="stSidebarContent"] {
        padding-top: 0rem !important; 
    }

    header {visibility: hidden; height: 0px !important;}
    footer {visibility: hidden;}

    html, body, [class*="css"], .stApp {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        background-color: #FFFFFF !important;
    }

    .main-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e3a8a 50%, #3b82f6 100%);
        padding: 1.2rem 2rem;
        border-radius: 15px;
        margin-bottom: 1rem;
        display: flex;
        flex-direction: column;
        align-items: center; 
        justify-content: center;
        box-shadow: 0 8px 20px rgba(0,0,0,0.15);
        text-align: center;
    }
    .main-header h1 { 
        color: white !important; 
        margin: 0 !important; 
        font-weight: 800; 
        font-size: 1.9rem; 
        text-transform: uppercase;
    }

    .sidebar-title {
        font-size: 0.95rem;
        font-weight: 800;
        color: #1e3a8a;
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        border-left: 4px solid #3b82f6;
        padding-left: 10px;
        margin-top: 1rem;
    }

    .kpi-card {
        background-color: #fdfdfd;
        border: 1px solid #e2e8f0;
        padding: 20px;
        border-radius: 12px;
        text-align: center;
        height: 100%;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02);
    }
    .kpi-card h2 { color: #1e3a8a !important; font-size: 1.4rem; margin: 10px 0; font-weight: 700; }
    .kpi-card span { color: #64748b !important; text-transform: uppercase; font-size: 0.8rem; font-weight: 600; letter-spacing: 0.5px; }
    
    .info-box {
        background: #f8fafc;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #3b82f6;
        margin-bottom: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

# ----------------------------------------------------
# 2. DATA LOADING
# ----------------------------------------------------
@st.cache_data
def load_data():
    try:
        # 1. ข้อมูลหลัก (Raw Data)
        df = pd.read_parquet('data/merged_dataset_FILLED.parquet')
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
        
        # 2. ข้อมูล DTW (Yearly)
        df_dtw = pd.read_parquet('data/dtw_results.parquet')
        df_dtw.columns = [c.lower() for c in df_dtw.columns]
        # สร้างคอลัมน์ date เป็นวันที่ 1 กรกฎาคมของปีนั้นๆ เพื่อให้จุดอยู่กลางปีเวลา plot
        df_dtw['date'] = pd.to_datetime(df_dtw['year'].astype(str) + '-07-01')
        
        # 3. GeoPandas (Khon Kaen)
        gdf = gpd.read_file('data/khonkaen_provinces.shp')
        gdf['geometry'] = gdf['geometry'].simplify(0.005)
        
        name_map = {
            'Subdistric': 'subdistrict', 'District': 'district', 'Province': 'province',
            'ADM3_EN': 'subdistrict', 'ADM2_EN': 'district', 'ADM1_EN': 'province',
            'amphoe_en': 'district', 'tambon_en': 'subdistrict', 'changwat_en': 'province'
        }
        gdf = gdf.rename(columns=name_map)
        
        for col in ['province', 'district', 'subdistrict']:
            for d in [df, df_dtw]:
                if col in d.columns: d[col] = d[col].astype(str).str.upper()
            if col in gdf.columns: gdf[col] = gdf[col].astype(str).str.upper()
            
        return df, gdf, df_dtw
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), gpd.GeoDataFrame(), pd.DataFrame()

df, gdf, df_dtw = load_data()


# ----------------------------------------------------
# 3. NAVIGATION & HEADER
# ----------------------------------------------------
if 'page' not in st.session_state: st.session_state.page = 'dashboard'
if 'playing' not in st.session_state: st.session_state.playing = False
if 'date_index' not in st.session_state: st.session_state.date_index = 0

st.markdown("""
    <div class='main-header'>
        <h1>SPATIAL-TEMPORAL DROUGHT MONITORING SYSTEM</h1>
        <p style='color: #bae6fd; margin: 5px 0 0 0; font-size: 0.9rem;'>Environmental Health & Vegetation Integrity Insights</p>
    </div>
""", unsafe_allow_html=True)


# ----------------------------------------------------
# 4. DASHBOARD PAGE
# 4.1 Main Page 
# ----------------------------------------------------
if st.session_state.page == 'dashboard' and not df.empty:
    
    # --- [SIDEBAR: CONFIGURATION & FILTERS] ---
    with st.sidebar:
        st.markdown("<div class='sidebar-title'>📊 INDICATOR</div>", unsafe_allow_html=True)
        indicators = {
            'ndvi': '🌿 NDVI (Vegetation)', 
            'lst': '🔥 LST (Surface Temp)', 
            'firecount': '🚨 FIRE COUNT', 
            'soilmoisture': '💧 SOIL MOISTURE', 
            'rainfall': '🌧️ RAINFALL'
        }
        selected_var = st.selectbox("INDICATOR", options=list(indicators.keys()), 
                                    format_func=lambda x: indicators[x], label_visibility="collapsed")

        st.markdown("<div class='sidebar-title'>📍 AREA SELECTION</div>", unsafe_allow_html=True)
        
        # 1. เลือก จังหวัด (ยังคงเป็น Multiselect หลัก)
        all_provs = sorted(df['province'].unique())
        sel_provs = st.multiselect("SELECT PROVINCES", all_provs, default=[all_provs[0]])
        
        sel_dists = []
        sel_subs = []

        # 2. จัดกลุ่ม อำเภอ และ ตำบล ภายใต้ Expander ของแต่ละจังหวัด
        if sel_provs:
            for prov in sel_provs:
                with st.expander(f"📍 {prov}", expanded=True):
                    # กรองอำเภอเฉพาะจังหวัดนี้
                    dists_in_prov = sorted(df[df['province'] == prov]['district'].unique())
                    prov_dists = st.multiselect(f"Districts in {prov}", dists_in_prov, key=f"dist_{prov}")
                    sel_dists.extend(prov_dists)
                    
                    # ถ้ามีการเลือกอำเภอในจังหวัดนี้ ให้แสดงการเลือกตำบล
                    if prov_dists:
                        for dist in prov_dists:
                            subs_in_dist = sorted(df[(df['province'] == prov) & (df['district'] == dist)]['subdistrict'].unique())
                            dist_subs = st.multiselect(f"└─ Subdistricts in {dist}", subs_in_dist, key=f"sub_{prov}_{dist}")
                            sel_subs.extend(dist_subs)
        else:
            st.info("Please select a province first.")

        st.divider()
        st.markdown("<div class='sidebar-title'>⏳ AUTO PLAY & TIMELINE</div>", unsafe_allow_html=True)

        
        # --- Animation Session State ---
        if 'playing' not in st.session_state: st.session_state.playing = False
        if 'date_index' not in st.session_state: st.session_state.date_index = 0

        col_p1, col_p2 = st.columns(2)
        if col_p1.button("▶️ Play" if not st.session_state.playing else "⏸️ Pause", use_container_width=True):
            st.session_state.playing = not st.session_state.playing
            st.rerun()
            
        if col_p2.button("Reset 🔄", use_container_width=True):
            st.session_state.date_index = 0
            st.session_state.playing = False
            st.rerun()

        play_speed = st.select_slider("Speed (sec)", options=[0.1, 0.3, 0.5, 1.0], value=0.3)

        # --- Timeline Logic ---
        all_dates = sorted(df['date'].unique())
        time_mode = st.radio("Selection Mode", ["Auto Play (Single)", "Manual Range"], horizontal=True)

        if time_mode == "Auto Play (Single)":
            selected_date = st.select_slider(
                "Current Month",
                options=all_dates,
                value=all_dates[st.session_state.date_index],
                format_func=lambda x: x.strftime('%b %Y'),
                key="animation_slider"
            )
            st.session_state.date_index = all_dates.index(selected_date)
            start_date = end_date = selected_date
        else:
            st.session_state.playing = False 
            date_range = st.select_slider(
                "Select Range",
                options=all_dates,
                value=(all_dates[0], all_dates[-1]),
                format_func=lambda x: x.strftime('%b %y')
            )
            start_date, end_date = date_range

    # --- [MAIN CONTENT: DATA PREPARATION] ---
    # 1. กรองข้อมูลตามพื้นที่ก่อน (เพื่อใช้ทำ Trend/Heatmap)
    dff_area = df.copy()
    if sel_provs: dff_area = dff_area[dff_area['province'].isin(sel_provs)]
    if sel_dists: dff_area = dff_area[dff_area['district'].isin(sel_dists)]
    if sel_subs:  dff_area = dff_area[dff_area['subdistrict'].isin(sel_subs)]

    # 2. กรองข้อมูลตามช่วงเวลา
    if time_mode == "Auto Play (Single)":
        # สำหรับ Map: แสดงเฉพาะเดือนปัจจุบัน
        dff_map = dff_area[dff_area['date'] == start_date]
        # สำหรับ Trend: แสดงตั้งแต่จุดเริ่มต้นจนถึงเดือนปัจจุบัน (เพื่อให้เส้นค่อยๆ ขยับ)
        dff_trend = dff_area[dff_area['date'] <= start_date]
        # สำหรับ KPI: แสดงเฉพาะเดือนปัจจุบัน
        dff_kpi = dff_map
    else:
        # โหมด Manual: แสดงตามช่วงที่เลือกปกติ
        dff_map = dff_area[dff_area['date'] == end_date]
        dff_trend = dff_area[(dff_area['date'] >= start_date) & (dff_area['date'] <= end_date)]
        dff_kpi = dff_trend

    if not dff_area.empty:
        # --- SECTION 1: KPI SUMMARY ---
        avg_v, min_v, max_v = dff_kpi[selected_var].mean(), dff_kpi[selected_var].min(), dff_kpi[selected_var].max()
        
        st.markdown(f"""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700;800&display=swap');
                .kpi-container {{ background: white; border-radius: 20px; padding: 30px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); border: 1px solid #f0f2f6; margin-bottom: 30px; font-family: 'Inter', sans-serif; }}
                .kpi-header {{ text-align: center; margin-bottom: 25px; padding-bottom: 15px; border-bottom: 2px solid #f8f9fa; }}
                .kpi-title {{ color: #1e293b; font-size: 1.4rem; font-weight: 800; letter-spacing: -0.5px; display: flex; align-items: center; justify-content: center; gap: 12px; }}
                .kpi-values-wrapper {{ display: flex; justify-content: space-around; align-items: center; gap: 20px; }}
                .kpi-box {{ flex: 1; text-align: center; }}
                .kpi-label {{ color: #64748b; font-size: 0.9rem; font-weight: 600; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; }}
                .kpi-number {{ font-size: 2.2rem; font-weight: 800; line-height: 1; }}
                .v-divider {{ width: 2px; height: 50px; background: #f1f5f9; }}
            </style>
            <div class='kpi-container'>
                <div class='kpi-header'>
                    <div class='kpi-title'>📊 STATISTICAL SUMMARY: {indicators[selected_var].upper()}</div>
                </div>
                <div class='kpi-values-wrapper'>
                    <div class='kpi-box'><div class='kpi-label'>Average</div><div class='kpi-number' style='color: #3b82f6;'>{avg_v:.3f}</div></div>
                    <div class='v-divider'></div>
                    <div class='kpi-box'><div class='kpi-label'>Minimum</div><div class='kpi-number' style='color: #10b981;'>{min_v:.3f}</div></div>
                    <div class='v-divider'></div>
                    <div class='kpi-box'><div class='kpi-label'>Maximum</div><div class='kpi-number' style='color: #ef4444;'>{max_v:.3f}</div></div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        # --- SECTION 2: MAP & TREND ---
        col_left, col_right = st.columns([2, 1.2])
        
        with col_left:
            time_title = f"{start_date.strftime('%B %Y')}" if time_mode == "Auto Play (Single)" else f"{start_date.strftime('%B %Y')} - {end_date.strftime('%B %Y')}"
            st.markdown(f"#### 🗺️ Spatial Distribution ({time_title})")
            
            # Map Logic: ใช้ข้อมูล dff_map (เดือนเดียว)
            df_map_latest = dff_map.groupby(['province', 'district', 'subdistrict'])[selected_var].mean().reset_index()
            merged_gdf = gdf.merge(df_map_latest, on=['province', 'district', 'subdistrict'], how='inner')
            
            if not merged_gdf.empty:
                province_boundary = merged_gdf.dissolve(by='province')
                bounds = merged_gdf.total_bounds
                center_lat, center_lon = (bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2
                
                # Dynamic Zoom
                max_diff = max(bounds[3] - bounds[1], bounds[2] - bounds[0])
                zoom_level = 11 if max_diff < 0.1 else 9 if max_diff < 0.5 else 8 if max_diff < 1.5 else 7
                
                map_themes = {'ndvi': 'YlGn', 'soilmoisture': 'Greens', 'rainfall': 'Blues', 'lst': 'OrRd'}
                map_theme = map_themes.get(selected_var, 'Reds')

                # แก้ไขส่วนการสร้าง fig_map
                fig_map = px.choropleth_mapbox(
                    merged_gdf, 
                    geojson=merged_gdf.geometry.__geo_interface__, 
                    locations=merged_gdf.index,
                    color=selected_var, 
                    color_continuous_scale=map_theme,
                    range_color=[df[selected_var].min(), df[selected_var].max()],
                    mapbox_style="carto-positron", 
                    center={"lat": center_lat, "lon": center_lon}, 
                    zoom=zoom_level, 
                    opacity=0.85, 
                    height=500,
                    # ✅ เพิ่มส่วนนี้เพื่อกำหนดข้อมูลที่จะแสดงใน Hover
                    hover_name='subdistrict', # หัวข้อหลักในกล่อง hover จะเป็นชื่อตำบล
                    hover_data={
                        'province': True,    # แสดงจังหวัด
                        'district': True,    # แสดงอำเภอ
                        'subdistrict': False, # ซ่อนตำบลซ้ำ (เพราะมีใน hover_name แล้ว)
                        selected_var: ':.4f'  # แสดงค่าตัวแปรพร้อมทศนิยม 4 ตำแหน่ง
                    }
                )

                # ปรับแต่ง hovertemplate ให้สวยงามและไม่มีคำว่า "index"
                fig_map.update_traces(
                    hovertemplate="<b>%{hovertext}</b><br>Province: %{customdata[0]}<br>District: %{customdata[1]}<br>Value: %{z:.4f}<extra></extra>"
                )

                # Add Province Borders
                for _, row in province_boundary.iterrows():
                    if row.geometry.geom_type == 'Polygon':
                        coords = list(row.geometry.exterior.coords)
                    elif row.geometry.geom_type == 'MultiPolygon':
                        coords = []
                        for poly in row.geometry.geoms:
                            coords.extend(list(poly.exterior.coords))
                            coords.append((None, None))
                    lons, lats = zip(*coords)
                    fig_map.add_trace(go.Scattermapbox(lon=lons, lat=lats, mode='lines', line=dict(width=2, color='#000000'), hoverinfo='skip', showlegend=False))

                fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, uirevision=selected_var)
                st.plotly_chart(fig_map, use_container_width=True)

        with col_right:
            st.markdown(f"#### 📈 Temporal Trend")
            group_col = 'subdistrict' if sel_subs else 'district' if sel_dists else 'province'
            
            # 1. เตรียมข้อมูล Trend (ค่อยๆ สะสมตามการ Play)
            trend_plot_data = dff_trend.groupby(['date', group_col])[selected_var].mean().reset_index()
            
            # 2. Logic ขยายแกน X จากตรงกลาง
            min_data_date = all_dates[0]
            max_data_date = all_dates[-1]
            current_date = start_date
            
            if time_mode == "Auto Play (Single)":
                # คำนวณ Margin ให้จุดปัจจุบันอยู่กึ่งกลาง (เผื่อไว้ด้านละ 6 เดือนเป็นอย่างน้อยเพื่อให้เห็นพื้นที่ว่าง)
                days_diff = (current_date - min_data_date).days
                view_margin = max(days_diff, 180) # 180 วันคือประมาณครึ่งปี
                
                x_range = [
                    current_date - pd.Timedelta(days=view_margin),
                    current_date + pd.Timedelta(days=view_margin)
                ]
            else:
                # โหมดปกติแสดงตามช่วงวันที่เลือก
                x_range = [start_date, end_date]
            
            fig_trend = px.line(
                trend_plot_data, 
                x='date', 
                y=selected_var, 
                color=group_col, 
                markers=True
            )
            
            fig_trend.update_layout(
                template="plotly_white", 
                height=500, 
                margin=dict(t=30, b=80, l=10, r=10),
                legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                xaxis=dict(
                    range=x_range,
                    showgrid=True,
                    title=None
                ),
                uirevision='constant' # รักษาค่า Zoom/Pan ไม่ให้กระพริบ
            )

            # 3. เพิ่มเส้นบอกตำแหน่งปัจจุบัน (เฉพาะตอนเล่น Auto Play)
            if time_mode == "Auto Play (Single)":
                fig_trend.add_vline(x=current_date, line_dash="dash", line_color="#FF4B4B", opacity=0.7)
            
            st.plotly_chart(fig_trend, use_container_width=True)

        # --- SECTION 3: HEATMAP ---
        st.divider()
        st.markdown(f"### 🌡️ Subdistrict Intensity Heatmap: {indicators[selected_var]}")
        
        # แสดงผลเต็มปีเสมอเพื่อให้เห็นภาพรวม
        display_start = pd.Timestamp(year=start_date.year, month=1, day=1)
        display_end = pd.Timestamp(year=end_date.year, month=12, day=1)

        for prov in dff_area['province'].unique():
            prov_data = df[(df['province'] == prov) & (df['date'] >= display_start) & (df['date'] <= display_end)]
            district_map = prov_data.drop_duplicates('subdistrict').set_index('subdistrict')['district'].to_dict()
            heat_pivot = prov_data.pivot_table(index='subdistrict', columns='date', values=selected_var, aggfunc='mean')
            
            if not heat_pivot.empty:
                fig_heat = px.imshow(
                    heat_pivot, 
                    color_continuous_scale=map_theme, 
                    title=f"Province: {prov}", 
                    aspect="auto"
                )

                # --- 1. Hover Configuration ---
                hover_texts = []
                for sub in heat_pivot.index:
                    dist = district_map.get(sub, "N/A")
                    row_hover = [
                        f"<b>Province:</b> {prov}<br><b>District:</b> {dist}<br><b>Subdistrict:</b> {sub}<br><b>Date:</b> {dt.strftime('%B %Y')}<br><b>Value:</b> {val:.4f}" 
                        for dt, val in heat_pivot.loc[sub].items()
                    ]
                    hover_texts.append(row_hover)
                fig_heat.update_traces(hovertemplate="%{customdata}<extra></extra>", customdata=hover_texts)

                # --- 2. จัดการเส้นแบ่งปี (Shapes) ---
                dates_list = heat_pivot.columns
                year_shapes = []
                tick_vals = []
                tick_text = []

                years_present = sorted(list(set([d.year for d in dates_list])))
                for yr in years_present:
                    year_dates = [d for d in dates_list if d.year == yr]
                    # เก็บค่าตำแหน่งกึ่งกลางปีไว้ทำ Tick Label
                    mid_date = year_dates[len(year_dates)//2]
                    tick_vals.append(mid_date)
                    tick_text.append(f"<b>{yr}</b>")
                    
                    # เส้นแบ่งแนวตั้งดำๆ ระหว่างปี
                    if yr != years_present[-1]:
                        div_pos = year_dates[-1] + pd.Timedelta(days=15)
                        year_shapes.append(dict(
                            type="line", x0=div_pos, x1=div_pos, y0=-0.5, y1=len(heat_pivot)-0.5,
                            line=dict(color="black", width=1.5)
                        ))

                # --- 3. ปรับ Layout ให้ตัวเลขปีเนียนไปกับชื่อตำบล ---
                fig_heat.update_layout(
                    height=400 + (len(heat_pivot) * 15),
                    margin=dict(t=50, b=80, r=100, l=100),
                    shapes=year_shapes,
                    xaxis=dict(
                        tickmode='array',
                        tickvals=tick_vals,
                        ticktext=tick_text, # แสดงเฉพาะปี
                        side='bottom',
                        showgrid=False,
                        title=None,
                        fixedrange=True,
                        # ปรับสีให้เป็นสีเทาเข้ม (สีเดียวกับชื่อตำบล) และลดความหนา
                        tickfont=dict(
                            size=10,           # ขนาดเล็กลงเท่าชื่อตำบล
                            color="rgba(0,0,0,0.6)", # สีเทาโปร่งแสงเล็กน้อยให้ดูซอฟต์
                            family="Arial"
                        )
                    ),
                    yaxis=dict(
                        autorange="reversed", 
                        title=None,
                        tickfont=dict(size=10, color="rgba(0,0,0,0.6)") # สีเดียวกัน
                    ),
                    yaxis2=dict(
                        overlaying="y", 
                        side="right", 
                        tickmode="linear",
                        tickvals=list(range(len(heat_pivot))),
                        ticktext=heat_pivot.index,
                        autorange="reversed",
                        tickfont=dict(size=10, color="rgba(0,0,0,0.6)") # สีเดียวกัน
                    )
                )


                st.plotly_chart(fig_heat, use_container_width=True)

        # --- AUTO-PLAY ENGINE ---
        if st.session_state.playing and time_mode == "Auto Play (Single)":
            if st.session_state.date_index < len(all_dates) - 1:
                st.session_state.date_index += 1
                time.sleep(play_speed)
                st.rerun()
            else:
                st.session_state.playing = False
                st.rerun()


# ----------------------------------------------------
# 4.2 DTW Analysis Page (With Auto Play & Timeline)
# ----------------------------------------------------
elif st.session_state.page == 'dtw' and not df_dtw.empty:
    
    # --- [SIDEBAR: CONFIGURATION & FILTERS] ---
    with st.sidebar:
        # 1. INDICATOR
        st.markdown("<div class='sidebar-title'>🔍 DTW INDICATOR</div>", unsafe_allow_html=True)
        dtw_vars = {
            'dtw_ndvi': '🌿 DTW NDVI', 
            'dtw_lst': '🔥 DTW LST', 
            'dtw_firecount': '🚨 DTW FIRE COUNT',
            'dtw_soilmoisture': '💧 DTW SOIL MOISTURE', 
            'dtw_rainfall': '🌧️ DTW RAINFALL'
        }
        selected_dtw = st.selectbox("DTW VAR", options=list(dtw_vars.keys()), 
                                    format_func=lambda x: dtw_vars[x], label_visibility="collapsed")
        flag_col = f"{selected_dtw}_flag"
        thresh_col = f"{selected_dtw}_threshold"

        st.divider()

        # 2. AREA SELECTION (อยู่บนสุดตามที่ต้องการ)
        st.markdown("<div class='sidebar-title'>📍 AREA SELECTION</div>", unsafe_allow_html=True)
        all_provs_dtw = sorted(df_dtw['province'].unique())
        sel_provs_dtw = st.multiselect("PROVINCES", all_provs_dtw, default=[all_provs_dtw[0]], key="dtw_prov")
        
        sub_df_prov = df_dtw[df_dtw['province'].isin(sel_provs_dtw)] if sel_provs_dtw else df_dtw
        all_dists_dtw = sorted(sub_df_prov['district'].unique())
        sel_dists_dtw = st.multiselect("DISTRICTS", all_dists_dtw, key="dtw_dist")
        
        sub_df_dist = sub_df_prov[sub_df_prov['district'].isin(sel_dists_dtw)] if sel_dists_dtw else sub_df_prov
        all_subs_dtw = sorted(sub_df_dist['subdistrict'].unique())
        sel_subs_dtw = st.multiselect("SUBDISTRICTS", all_subs_dtw, key="dtw_sub")

        st.divider()

        # 3. AUTO PLAY & TIMELINE (ยกมาจากหน้าหลัก)
        st.markdown("<div class='sidebar-title'>⏳ AUTO PLAY & TIMELINE</div>", unsafe_allow_html=True)
        
        if 'dtw_playing' not in st.session_state: st.session_state.dtw_playing = False
        if 'dtw_year_index' not in st.session_state: st.session_state.dtw_year_index = 0

        col_p1, col_p2 = st.columns(2)
        if col_p1.button("▶️ Play" if not st.session_state.dtw_playing else "⏸️ Pause", key="dtw_play_btn", use_container_width=True):
            st.session_state.dtw_playing = not st.session_state.dtw_playing
            st.rerun()
            
        if col_p2.button("Reset 🔄", key="dtw_reset_btn", use_container_width=True):
            st.session_state.dtw_year_index = 0
            st.session_state.dtw_playing = False
            st.rerun()

        play_speed = st.select_slider("Speed (sec)", options=[0.1, 0.3, 0.5, 1.0], value=0.3, key="dtw_speed")

        all_years = sorted(df_dtw['year'].unique())
        time_mode = st.radio("Selection Mode", ["Auto Play (Single)", "Manual Range"], horizontal=True, key="dtw_time_mode")

        if time_mode == "Auto Play (Single)":
            selected_year = st.select_slider(
                "Current Year",
                options=all_years,
                value=all_years[st.session_state.dtw_year_index],
                key="dtw_animation_slider"
            )
            st.session_state.dtw_year_index = all_years.index(selected_year)
            start_yr = end_yr = selected_year
        else:
            st.session_state.dtw_playing = False 
            year_range = st.select_slider(
                "Select Range",
                options=all_years,
                value=(all_years[0], all_years[-1]),
                key="dtw_range_slider"
            )
            start_yr, end_yr = year_range

    # --- [MAIN CONTENT: DATA PREPARATION] ---
    # 1. กรองพื้นที่ก่อน
    dff_area = df_dtw.copy()
    if sel_provs_dtw: dff_area = dff_area[dff_area['province'].isin(sel_provs_dtw)]
    if sel_dists_dtw: dff_area = dff_area[dff_area['district'].isin(sel_dists_dtw)]
    if sel_subs_dtw:  dff_area = dff_area[dff_area['subdistrict'].isin(sel_subs_dtw)]

    # 2. กรองตามเวลา (Logic เดียวกับหน้าหลัก)
    if time_mode == "Auto Play (Single)":
        dff_map = dff_area[dff_area['year'] == start_yr]
        dff_trend = dff_area[dff_area['year'] <= start_yr]
        dff_kpi = dff_map
    else:
        dff_map = dff_area[dff_area['year'] == end_yr]
        dff_trend = dff_area[(dff_area['year'] >= start_yr) & (dff_area['year'] <= end_yr)]
        dff_kpi = dff_trend

    

    if not dff_kpi.empty:
        # --- SECTION 1: KPI SUMMARY (Anomaly Focus - Dashboard Match Design) ---
        avg_dist = dff_kpi[selected_dtw].mean()
        anomaly_count = dff_kpi[dff_kpi[flag_col] == 1].shape[0]
        anomaly_pct = (anomaly_count / len(dff_kpi) * 100) if len(dff_kpi) > 0 else 0

        st.markdown(f"""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700;800&display=swap');
                .kpi-dtw-container {{ 
                    background: white; 
                    border-radius: 20px; 
                    padding: 30px; 
                    box-shadow: 0 10px 25px rgba(0,0,0,0.05); 
                    border: 1px solid #f0f2f6; 
                    margin-bottom: 30px; 
                    font-family: 'Inter', sans-serif; 
                }}
                .kpi-dtw-header {{ 
                    text-align: center; 
                    margin-bottom: 25px; 
                    padding-bottom: 15px; 
                    border-bottom: 2px solid #f8f9fa; 
                }}
                .kpi-dtw-title {{ 
                    color: #1e293b; 
                    font-size: 1.4rem; 
                    font-weight: 800; 
                    letter-spacing: -0.5px; 
                    display: flex; 
                    align-items: center; 
                    justify-content: center; 
                    gap: 12px; 
                }}
                .kpi-dtw-values-wrapper {{ 
                    display: flex; 
                    justify-content: space-around; 
                    align-items: center; 
                    gap: 20px; 
                }}
                .kpi-dtw-box {{ flex: 1; text-align: center; }}
                .kpi-dtw-label {{ 
                    color: #64748b; 
                    font-size: 0.9rem; 
                    font-weight: 600; 
                    margin-bottom: 8px; 
                    text-transform: uppercase; 
                    letter-spacing: 1px; 
                }}
                .kpi-dtw-number {{ 
                    font-size: 2.2rem; 
                    font-weight: 800; 
                    line-height: 1; 
                }}
                .v-divider-dtw {{ width: 2px; height: 50px; background: #f1f5f9; }}
            </style>
            
            <div class='kpi-dtw-container'>
                <div class='kpi-dtw-header'>
                    <div class='kpi-dtw-title'>📊 ANOMALY INSIGHTS: {dtw_vars[selected_dtw].upper()}</div>
                </div>
                <div class='kpi-dtw-values-wrapper'>
                    <div class='kpi-dtw-box'>
                        <div class='kpi-dtw-label'>Avg Distance</div>
                        <div class='kpi-dtw-number' style='color: #3b82f6;'>{avg_dist:.3f}</div>
                    </div>
                    <div class='v-divider-dtw'></div>
                    <div class='kpi-dtw-box'>
                        <div class='kpi-dtw-label'>Anomaly Cases</div>
                        <div class='kpi-dtw-number' style='color: #ef4444;'>{anomaly_count}</div>
                    </div>
                    <div class='v-divider-dtw'></div>
                    <div class='kpi-dtw-box'>
                        <div class='kpi-dtw-label'>Anomaly Rate</div>
                        <div class='kpi-dtw-number' style='color: #d97706;'>{anomaly_pct:.1f}%</div>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        # --- SECTION 2: MAP & TREND ---
        
        col_l, col_r = st.columns([2, 1.2])
        
        with col_l:
            time_title = f"{start_yr}" if time_mode == "Auto Play (Single)" else f"{start_yr} - {end_yr}"
            st.markdown(f"#### 🗺️ Spatial Anomaly ({time_title})")
            
            # 1. กรองข้อมูลเฉพาะปีที่เลือกและลบแถวที่ไม่มีข้อมูลสำคัญ
            df_map_dtw = dff_map.copy()
            merged_dtw = gdf.merge(df_map_dtw, on=['province', 'district', 'subdistrict'], how='inner')
            
            # ตรวจสอบว่ามีข้อมูลและมีคอลัมน์ครบไหมก่อนรัน Map
            if not merged_dtw.empty and flag_col in merged_dtw.columns and selected_dtw in merged_dtw.columns:
                
                # --- 🎯 ส่วนการคำนวณหาจุดกึ่งกลางและเส้นขอบจังหวัด ---
                province_boundary = merged_dtw.dissolve(by='province')
                bounds = merged_dtw.total_bounds
                center_lat = (bounds[1] + bounds[3]) / 2
                center_lon = (bounds[0] + bounds[2]) / 2
                
                max_diff = max(bounds[2] - bounds[0], bounds[3] - bounds[1])
                zoom_level = 11 if max_diff < 0.1 else 9.5 if max_diff < 0.5 else 8.5 if max_diff < 1.5 else 7.5
                

                # 2. สร้างแผนที่
                
                #  สร้างคอลัมน์สำหรับแสดงสถานะใน Hover (Display Column)
                merged_dtw['status'] = merged_dtw[flag_col].apply(lambda x: '🚨 Abnormal' if x == 1 else '✅ Normal')

                #  สร้างแผนที่
                fig_map_dtw = px.choropleth_mapbox(
                    merged_dtw, 
                    geojson=merged_dtw.geometry.__geo_interface__, 
                    locations=merged_dtw.index,
                    color=flag_col,
                    color_continuous_scale=[[0, '#e2e8f0'], [1, '#ef4444']], 
                    mapbox_style="carto-positron", 
                    zoom=zoom_level, 
                    center={"lat": center_lat, "lon": center_lon}, 
                    height=500, 
                    opacity=0.8,
                    hover_name='subdistrict', 
                    # ระบุคอลัมน์ที่ต้องการให้ปรากฏใน Hover
                    hover_data={
                        'province': True, 
                        'district': True,
                        'status': True, # แสดงผลภาษาไทย/อังกฤษตามที่ตั้งไว้
                        selected_dtw: ':.4f',
                        flag_col: False # ปิดเลข 0, 1 เดิม
                    }
                )

                # ปรับแต่งหัวข้อใน Hover ให้สวยงามยิ่งขึ้น
                fig_map_dtw.update_traces(
                    hovertemplate="<b>%{hovertext}</b><br>" +
                                  "Province: %{customdata[0]}<br>" +
                                  "District: %{customdata[1]}<br>" +
                                  "Status: %{customdata[2]}<br>" +
                                  "Value: %{customdata[3]:.4f}<extra></extra>"
                )

                # 3. 🔥 เพิ่มเส้นขอบจังหวัด (Province Borders)
                for _, row in province_boundary.iterrows():
                    if row.geometry.geom_type == 'Polygon':
                        coords = list(row.geometry.exterior.coords)
                    elif row.geometry.geom_type == 'MultiPolygon':
                        coords = []
                        for poly in row.geometry.geoms:
                            coords.extend(list(poly.exterior.coords))
                            coords.append((None, None))
                    
                    if coords:
                        lons, lats = zip(*coords)
                        fig_map_dtw.add_trace(go.Scattermapbox(
                            lon=lons, lat=lats, 
                            mode='lines', 
                            line=dict(width=2, color='#000000'),
                            hoverinfo='skip', 
                            showlegend=False
                        ))

                fig_map_dtw.update_layout(
                    margin={"r":0,"t":0,"l":0,"b":0}, 
                    coloraxis_showscale=False,
                    # ใช้ uirevision ผูกกับสถานที่เพื่อให้แผนที่ขยับเมื่อเปลี่ยนที่ แต่ไม่ขยับเมื่อเล่น Auto Play
                    uirevision=f"{sel_provs_dtw}-{sel_dists_dtw}-{sel_subs_dtw}"
                )
                
                st.plotly_chart(fig_map_dtw, use_container_width=True)
            else:
                # กรณีข้อมูลว่างเปล่า ให้แสดงกล่องข้อความแทนการปล่อยให้ Error
                st.info(f"ℹ️ No data available for {selected_dtw} in the selected period/area.")

        with col_r:
            st.markdown(f"#### 📈 Distance vs Threshold")
            
            # 1. เตรียมข้อมูลแบบสะสม (ใช้ dff_trend)
            trend_plot_data = dff_trend.groupby('year')[[selected_dtw, thresh_col]].mean().reset_index()
            
            # 2. Logic จัดการแกน X 
            current_yr = start_yr
            first_year_ever = all_years[0]
            
            if time_mode == "Auto Play (Single)":
                # ให้เห็นย้อนหลัง และเผื่อที่ว่างด้านหน้าเล็กน้อยเพื่อให้หัวอ่านไม่อยู่ชิดขอบเกินไป
                year_diff = current_yr - first_year_ever
                view_margin = max(year_diff, 2)
                x_range = [current_yr - view_margin, current_yr + 1] # +1 เพื่อให้เห็นปลายเส้นชัดๆ
            else:
                x_range = [start_yr, end_yr]
            
            fig_trend_dtw = go.Figure()
            
            # --- 📈 เส้น Distance (สิ้นสุดที่ปีปัจจุบัน) ---
            fig_trend_dtw.add_trace(go.Scatter(
                x=trend_plot_data['year'], 
                y=trend_plot_data[selected_dtw], 
                name="Distance",
                mode='lines+markers',
                line=dict(color='#3b82f6', width=4),
                marker=dict(size=10, line=dict(width=2, color='white'))
            ))
            
            # --- 🚨 เส้น Threshold (ปรับให้สิ้นสุดที่ปีปัจจุบันเหมือนกัน) ---
            fig_trend_dtw.add_trace(go.Scatter(
                x=trend_plot_data['year'], # ใช้ x เดียวกับ Distance ข้อมูลจะสิ้นสุดพร้อมกัน
                y=trend_plot_data[thresh_col], 
                name="Threshold",
                mode='lines',
                line=dict(color='#ef4444', dash='dash', width=2)
            ))
            
            # 3. Layout Configuration
            fig_trend_dtw.update_layout(
                template="plotly_white", 
                height=500, 
                margin=dict(t=30, b=80, l=10, r=10),
                legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                xaxis=dict(
                    range=x_range,
                    showgrid=True,
                    dtick=1,
                    title=None,
                    tickformat='d'
                ),
                yaxis=dict(title="Value"),
                uirevision='constant'
            )

            # 4. เส้น V-Line บอกตำแหน่งปีปัจจุบัน (หัวอ่าน)
            if time_mode == "Auto Play (Single)":
                fig_trend_dtw.add_vline(x=current_yr, line_dash="dash", line_color="#FF4B4B", opacity=0.7)
            
            st.plotly_chart(fig_trend_dtw, use_container_width=True)

        # --- SECTION 3: YEARLY HEATMAP (With Grid Lines) ---
        st.divider()
        st.markdown(f"### 🌡️ Yearly DTW Heatmap")
        
        for prov in dff_trend['province'].unique():
            # ดึงข้อมูล Pivot Table (จะสะสมปีตาม dff_trend)
            heat_dtw_pivot = dff_trend[dff_trend['province'] == prov].pivot_table(
                index='subdistrict', 
                columns='year', 
                values=selected_dtw
            )
            
            if not heat_dtw_pivot.empty:
                # สร้าง Heatmap
                fig_heat_dtw = px.imshow(
                    heat_dtw_pivot, 
                    color_continuous_scale="Reds", 
                    title=f"Province: {prov}", 
                    aspect="auto",
                    
                )
                
                # --- 🛠️ เพิ่มเส้นแบ่งปีและช่องว่าง ---
                fig_heat_dtw.update_traces(
                    xgap=2, # เพิ่มช่องว่างแนวตั้งระหว่างปี
                    ygap=2, # เพิ่มช่องว่างแนวนอนระหว่างตำบล
                    hovertemplate="Subdistrict: %{y}<br>Year: %{x}<br>Value: %{z:.4f}<extra></extra>"
                )
                
                fig_heat_dtw.update_layout(
                    height=400 + (len(heat_dtw_pivot) * 20), # ปรับความสูงตามจำนวนตำบล
                    yaxis=dict(autorange="reversed", title=None), 
                    xaxis=dict(
                        dtick=1, 
                        title=None, 
                        side="top", # เอาปีไว้ด้านบนให้ดูง่ายขึ้น
                        tickformat='d'
                    ),
                    margin=dict(l=10, r=10, t=50, b=10),
                    uirevision='constant' # ป้องกันการกระพริบขณะ Auto Play
                )
                
                st.plotly_chart(fig_heat_dtw, use_container_width=True)

        # --- [AUTO-PLAY ENGINE: AT THE END OF PAGE] ---
    if st.session_state.dtw_playing and time_mode == "Auto Play (Single)":
        if st.session_state.dtw_year_index < len(all_years) - 1:
            st.session_state.dtw_year_index += 1
            import time
            time.sleep(play_speed)
            st.rerun()
        else:
            st.session_state.dtw_playing = False
            st.rerun()
# ----------------------------------------------------
# 5. ABOUT PROJECT PAGE 
# ----------------------------------------------------
elif st.session_state.page == 'about':
    st.markdown("<h2 style='text-align: center; color: #1e3a8a;'>PROJECT OVERVIEW & TEAM</h2>", unsafe_allow_html=True)
    
    col_a1, col_a2 = st.columns([1, 1])
    with col_a1:
        st.markdown("### 🔍 Variable Guide")
        st.markdown("""
        <div class='info-box'>
            <b>🌿 NDVI:</b> ประเมินความสมบูรณ์ของพืชพรรณ<br>
            <b>🔥 LST:</b> อุณหภูมิพื้นผิวโลก (ความร้อนพื้นผิว)<br>
            <b>🚨 Fire Count:</b> จำนวนจุดความร้อน (Hotspots)<br>
            <b>💧 Soil Moisture:</b> ความชื้นในดิน<br>
            <b>🌧️ Rainfall:</b> ปริมาณน้ำฝนรายเดือน
        </div>
        """, unsafe_allow_html=True)
    
    with col_a2:
        st.markdown("### 🌍 Technical Sources")
        data_info = {
            "Variable": ["NDVI", "LST", "Rainfall", "SoilMoisture", "Fire Count"],
            "Source": ["MOD13Q1", "MOD11A2", "CHIRPS", "SMAP", "MOD14A1"]
        }
        st.table(pd.DataFrame(data_info))

# ----------------------------------------------------
# 6. BOTTOM NAVIGATION
# ----------------------------------------------------
st.markdown("<br>", unsafe_allow_html=True)
nav_cols = st.columns(3)
if nav_cols[0].button("🏠 MAIN DASHBOARD", use_container_width=True):
    st.session_state.page = 'dashboard'
    st.rerun()
if nav_cols[1].button("🔍 DTW ANALYSIS", use_container_width=True):
    st.session_state.page = 'dtw'
    st.rerun()
if nav_cols[2].button("ℹ️ ABOUT US", use_container_width=True):
    st.session_state.page = 'about'
    st.rerun()
