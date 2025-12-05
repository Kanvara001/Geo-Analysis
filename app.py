import dash
from dash import html
import geopandas as gpd
import pandas as pd
import folium

# -------------------------
# 1) Load your data
# -------------------------
shapefile_path = r"data/khonkaen_provinces.shp"
excel_path = r"data/df_merged_subdistrict.xlsx"

# Load shapefile
gdf = gpd.read_file(shapefile_path)

# Fix column name (shp has "Subdistric" but excel has "Subdistrict")
gdf = gdf.rename(columns={"Subdistric": "Subdistrict"})

# Load excel file
df = pd.read_excel(excel_path)

# Make sure the column names are clean (trim spaces)
df.columns = df.columns.str.strip()
gdf.columns = gdf.columns.str.strip()

# -------------------------
# 2) Merge shapefile + data
# -------------------------
merge_cols = ["Province", "District", "Subdistrict"]

# Ensure both datasets have all columns
for col in merge_cols:
    if col not in gdf.columns:
        print("❌ Missing col in gdf:", col)
    if col not in df.columns:
        print("❌ Missing col in df:", col)

gdf_merged = gdf.merge(df, on=merge_cols, how="left")

# -------------------------
# 3) Generate Folium Map
# -------------------------
m = folium.Map(location=[16.45, 102.83], zoom_start=9)

# Add Choropleth only if we have NDVI
if "NDVI" in gdf_merged.columns:
    folium.Choropleth(
        geo_data=gdf_merged,
        data=gdf_merged,
        columns=["Subdistrict", "NDVI"],
        key_on="feature.properties.Subdistrict",
        fill_color="YlGn",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="NDVI",
    ).add_to(m)

map_html = m._repr_html_()

# -------------------------
# 4) Dash App
# -------------------------
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Spatial Dashboard — Khon Kaen"),
    html.Iframe(srcDoc=map_html, width="100%", height="600")
])

# -------------------------
# 5) Run Server
# -------------------------
if __name__ == "__main__":
    app.run(debug=True)
