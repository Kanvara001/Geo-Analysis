import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import time

# ----------------------------------------------------
# 1. TOTAL UI RE-ENGINEERING (HIGH CONTRAST CSS)
# ----------------------------------------------------
st.set_page_config(page_title="SPATIAL ANALYTICS | DSS", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
    
    /* Global Styles */
    html, body, [class*="css"], .stApp {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        font-family: 'Inter', sans-serif !important;
    }

    /* Force all labels and text to Black */
    label, p, span, div, h1, h2, h3, h4 { color: #000000 !important; font-weight: 600 !important; }
    
    /* KPI & Filter Borders */
    .stMetric, .filter-box {
        border: 2px solid #000000 !important;
        padding: 15px !important;
        border-radius: 0px !important;
        background-color: #FFFFFF !important;
        margin-bottom: 10px !important;
    }

    /* Fixed Overlapping Header */
    .header-text { margin-bottom: 25px; padding: 10px; border-left: 10px solid #000; }

    /* Button Styling (About Project) */
    .stButton > button {
        background-color: #000000 !important;
        color: #FFFFFF !important;
        border: 2px solid #000000 !important;
        border-radius: 0px !important;
        font-weight: 800 !important;
        font-size: 14px !important;
        height: 50px;
    }
    .stButton > button:hover { background-color: #333333 !important; }

    /* About Us Card Design */
    .author-card {
        border: 1px solid #000;
        padding: 20px;
        text-align: center;
        background-color: #F9F9F9;
    }
    .author-img {
        width: 150px; height: 150px; border-radius: 50%; border: 3px solid #000;
        margin-bottom: 15px; background-color: #DDD; display: inline-block;
    }
    </style>
    """, unsafe_allow_html=True)

# ----------------------------------------------------
# 2. DATA LOAD & MEMORY OPTIMIZATION
# ----------------------------------------------------
@st.cache_data
def load_data():
    parquet_path = r'C:\Users\NBODT\my_dash_app\data\merged_dataset_FILLED.parquet'
    shp_path = r'C:\Users\NBODT\my_dash_app\data\khonkaen_provinces.shp'
    
    df = pd.read_parquet(parquet_path, engine='pyarrow')
    df.columns = [c.lower() for c in df.columns]
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    
    gdf = gpd.read_file(shp_path)
    gdf['geometry'] = gdf['geometry'].simplify(0.001, preserve_topology=True) # Prevent MemoryError
    
    if 'Subdistric' in gdf.columns:
        gdf = gdf.rename(columns={'Subdistric': 'subdistrict', 'District': 'district', 'Province': 'province'})
    
    for col in ['province', 'district', 'subdistrict']:
        df[col] = df[col].astype(str).str.upper()
        gdf[col] = gdf[col].astype(str).str.upper()
        
    return df, gdf

df, gdf = load_data()

# ----------------------------------------------------
# 3. NAVIGATION STATE
# ----------------------------------------------------
if 'page' not in st.session_state: st.session_state.page = 'dashboard'
if 'play_idx' not in st.session_state: st.session_state.play_idx = 0

col_h1, col_h2 = st.columns([3.5, 1])
with col_h1:
    st.markdown("<div class='header-text'><h1>SPATIAL ANALYSIS PLATFORM</h1><p>VERSION 3.0 | HIGH-PRECISION MONITORING</p></div>", unsafe_allow_html=True)
with col_h2:
    label = "ABOUT PROJECT" if st.session_state.page == 'dashboard' else "BACK TO DASHBOARD"
    if st.button(label):
        st.session_state.page = 'about' if st.session_state.page == 'dashboard' else 'dashboard'
        st.rerun()

# ----------------------------------------------------
# 4. DASHBOARD PAGE
# ----------------------------------------------------
if st.session_state.page == 'dashboard':
    
    # Sidebar - Structured with Containers
    with st.sidebar:
        st.markdown("### FILTER PARAMETERS")
        with st.container(border=True):
            indicators = {'ndvi': 'NDVI', 'lst': 'LST', 'soilmoisture': 'SOIL MOISTURE', 'rainfall': 'RAINFALL'}
            selected_var = st.selectbox("VARIABLE", options=list(indicators.keys()), format_func=lambda x: indicators[x])
            
            sel_prov = st.selectbox("PROVINCE", ["ALL PROVINCES"] + sorted(df['province'].unique()))
            dist_list = sorted(df[df['province'] == sel_prov]['district'].unique()) if sel_prov != "ALL PROVINCES" else []
            sel_dist = st.selectbox("DISTRICT", ["ALL DISTRICTS"] + dist_list)

        st.markdown("### MAP SETTINGS")
        with st.container(border=True):
            map_opacity = st.slider("OPACITY", 0.0, 1.0, 0.8, 0.05)
        
        st.markdown("### TIMELINE CONTROL")
        with st.container(border=True):
            all_dates = sorted(df['date'].unique())
            play_mode = st.checkbox("AUTO-PLAY (SEQUENCE)")
            if play_mode:
                st.session_state.play_idx = (st.session_state.play_idx + 1) % len(all_dates)
                start_date = all_dates[st.session_state.play_idx]
                end_date = start_date
                time.sleep(0.6)
                st.rerun()
            else:
                date_range = st.select_slider("SELECT RANGE", options=all_dates, value=(all_dates[0], all_dates[-1]), format_func=lambda x: x.strftime('%Y-%m'))
                start_date, end_date = date_range

    # Data Filtering
    dff = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    if sel_prov != "ALL PROVINCES":
        dff = dff[dff['province'] == sel_prov]
        if sel_dist != "ALL DISTRICTS":
            dff = dff[dff['district'] == sel_dist]

    # KPI ROW - BORDERED
    df_map = dff.groupby(['province', 'district', 'subdistrict'])[selected_var].mean().reset_index()
    if not df_map.empty:
        c1, c2, c3, c4 = st.columns(4)
        avg_val = df_map[selected_var].mean()
        max_row = df_map.loc[df_map[selected_var].idxmax()]
        min_row = df_map.loc[df_map[selected_var].idxmin()]

        with c1: st.markdown(f"<div class='stMetric'>AVG {selected_var.upper()}<br><h2>{avg_val:,.4f}</h2></div>", unsafe_allow_html=True)
        with c2: st.markdown(f"<div class='stMetric'>MAX {selected_var.upper()}<br><h2>{max_row[selected_var]:,.4f}</h2><small>{max_row['subdistrict']}, {max_row['district']}</small></div>", unsafe_allow_html=True)
        with c3: st.markdown(f"<div class='stMetric'>MIN {selected_var.upper()}<br><h2>{min_row[selected_var]:,.4f}</h2><small>{min_row['subdistrict']}, {min_row['district']}</small></div>", unsafe_allow_html=True)
        with c4: st.markdown(f"<div class='stMetric'>OBSERVATION<br><h2>{start_date.strftime('%b %Y')}</h2></div>", unsafe_allow_html=True)

    # --- MAP ---
    merged_gdf = gdf.merge(df_map, on=['province', 'district', 'subdistrict'], how='left')
    fig_map = px.choropleth_map(
        merged_gdf, geojson=merged_gdf.geometry.__geo_interface__, locations=merged_gdf.index,
        color=selected_var, color_continuous_scale='YlGnBu' if selected_var=='rainfall' else 'Viridis',
        center={"lat": merged_gdf.geometry.centroid.y.mean(), "lon": merged_gdf.geometry.centroid.x.mean()},
        map_style="open-street-map", zoom=7.5, opacity=map_opacity, height=600
    )
    fig_map.update_traces(marker_line_width=1.0, marker_line_color="black")
    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_map, use_container_width=True)

    # --- INSIGHT TREND ---
    st.markdown("### TEMPORAL ANALYTICS: TREND OVERVIEW")
    trend_data = dff.groupby('date')[selected_var].mean().reset_index()
    fig_trend = px.line(trend_data, x='date', y=selected_var, markers=True)
    fig_trend.update_layout(template="plotly_white", font=dict(color="black", size=14))
    fig_trend.update_traces(line=dict(color='black', width=3))
    st.plotly_chart(fig_trend, use_container_width=True)

# ----------------------------------------------------
# 5. PREMIUM ABOUT PROJECT & TEAM PAGE
# ----------------------------------------------------
else:
    st.markdown("## PROJECT DOCUMENTATION & TEAM")
    st.markdown("---")
    
    st.markdown("### THE MISSION")
    st.info("Developing a high-resolution Spatial Decision Support System (DSS) for drought monitoring and environmental health analysis using advanced remote sensing and multi-temporal data sets.")
    
    st.markdown("<br>### MEET THE TEAM", unsafe_allow_html=True)
    
    # Author Profiles with CSS Cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
            <div class='author-card'>
                <div class='author-img'></div>
                <h4>DR. PICHAYA WIRATCHOTISETTHIAN</h4>
                <p><b>Principal Advisor</b></p>
                <p>Faculty of Science, KKU</p>
                <p style='font-size:12px;'>PITCWI@KKU.AC.TH</p>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
            <div class='author-card'>
                <div class='author-img'></div>
                <h4>KANOKNAT KHRUANATE</h4>
                <p><b>Research Author</b></p>
                <p>Statistics & Data Science, KKU</p>
                <p style='font-size:12px;'>KANOKNAT.KR@KKUMAIL.COM</p>
            </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
            <div class='author-card'>
                <div class='author-img'></div>
                <h4>KANVARA THAWARORIT</h4>
                <p><b>Research Author</b></p>
                <p>Statistics & Data Science, KKU</p>
                <p style='font-size:12px;'>KANVARA.K@KKUMAIL.COM</p>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>### TECHNICAL SPECIFICATIONS", unsafe_allow_html=True)
    spec_df = pd.DataFrame({
        "COMPONENT": ["DATA ENGINE", "GIS PROCESSING", "VISUALIZATION", "RESOLUTION"],
        "TECHNOLOGY": ["PARQUET / PYARROW", "GEOPANDAS / SHAPELY", "PLOTLY GRAPH OBJECTS", "SUB-DISTRICT LEVEL"]
    })
    st.table(spec_df)