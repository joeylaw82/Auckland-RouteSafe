import pandas as pd
import geopandas as gpd
import requests
import io
import os
import json
from datetime import datetime
import re
from time import sleep 
from urllib.parse import urlencode 
import sys 
from shapely.geometry import Point


# --- 1. Configuration ---
POLICE_DATA_URL = os.environ.get("POLICE_DATA_URL") 
MESHBLOCK_BASE_URL = "https://services.arcgis.com/XTtANUDT8Va4DLwI/arcgis/rest/services/nz_meshblocks/FeatureServer/0"
ARCGIS_ROUTES_URL = "https://services2.arcgis.com/JkPEgZJGxhSjYOo0/arcgis/rest/services/BusService/FeatureServer/2/query?where=1%3D1&outFields=*&f=geojson"
ARCGIS_STOPS_URL = "https://services2.arcgis.com/JkPEgZJGxhSjYOo0/ArcGIS/rest/services/BusService/FeatureServer/0" 

AUCKLAND_AUTHORITIES = ['Auckland','Waitemata', 'Counties Manukau', 'Franklin', 'Auckland City'] 

# Output file paths
OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'route_crime_stats.geojson')
STATS_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'crime_breakdown.json')

# Max records per ArcGIS request for stability
MAX_RECORDS = 500 
TARGET_CRS = "EPSG:2193" # NZTM2000 for metric spatial operations


# --- 2. Helper Functions ---

def clean_territorial_authority(name: str) -> str:
    """Cleans up the territorial authority name."""
    if pd.isna(name): return ''
    cleaned = re.sub(r'[^\w\s]', '', str(name), flags=re.UNICODE) 
    cleaned = re.sub(r'\s+', ' ', cleaned).strip() 
    return cleaned.upper()

AUCKLAND_AUTHORITIES_CLEANED = [clean_territorial_authority(name) for name in AUCKLAND_AUTHORITIES]


def fetch_arcgis_geometry(base_url: str, id_field: str, out_fields: list, mode='geojson') -> gpd.GeoDataFrame:
    """Generic function to fetch geometry data from ArcGIS REST service using pagination."""
    print(f"   -> Fetching {id_field} geometry with pagination...")
    
    out_fields_str = ','.join(out_fields)
    count_url = f"{base_url}/query?where=1%3D1&returnCountOnly=true&f=json"
    
    try:
        count_response = requests.get(count_url)
        count_response.raise_for_status()
        total_count = count_response.json().get('count', 0)
        print(f"   -> Service reports total records: {total_count}")
        if total_count == 0:
            print(f"❌ Error: ArcGIS service reported zero records for {id_field}.")
            return gpd.GeoDataFrame()
    except Exception as e:
        print(f"❌ Failed to get total count for {id_field}: {e}")
        return gpd.GeoDataFrame()

    all_geometry = []
    offset = 0
    
    while offset < total_count:
        print(f"   -> Fetching batch: records {offset} to {min(offset + MAX_RECORDS, total_count)}...")
        
        query_params = {
            'where': '1=1',
            'outFields': out_fields_str,
            'resultOffset': offset,
            'resultRecordCount': MAX_RECORDS,
            'f': mode,
            'inSR': '4326', 
            'outSR': '4326',
        }
        
        query_url = f"{base_url}/query?{urlencode(query_params)}"
        
        try:
            response = requests.get(query_url)
            response.raise_for_status()
            
            # --- BUS STOP CRS FIX: Handle NZTM2000 X/Y coordinates from JSON service ---
            if mode == 'json' and 'BusService/FeatureServer/0' in base_url:
                data = response.json()
                features = data.get('features', [])
                if not features:
                    print("   -> 🚨 Warning: ArcGIS service returned an empty batch (JSON). Stopping fetch.")
                    break
                    
                df_batch = pd.DataFrame([f['attributes'] for f in features])
                
                # Bus stop coordinates are in NZTM2000 (EPSG:2193)
                geometry = [Point(f['geometry']['x'], f['geometry']['y']) for f in features]
                gdf_batch = gpd.GeoDataFrame(df_batch, geometry=geometry, crs="EPSG:2193") 
                
            # --- Standard GeoJSON (Routes and Meshblocks) ---
            else: 
                gdf_batch = gpd.read_file(io.BytesIO(response.content))
                
                if gdf_batch.empty:
                    print("   -> 🚨 Warning: ArcGIS service returned an empty batch (GeoJSON). Stopping fetch.")
                    break
                
            all_geometry.append(gdf_batch)
            offset += len(gdf_batch)
            sleep(0.5) 
            
        except Exception as e:
            print(f"❌ Failed to fetch batch data (Offset: {offset}): {e}")
            break
            
    if not all_geometry:
        print(f"❌ Error: Failed to retrieve any {id_field} data.")
        return gpd.GeoDataFrame()
        
    gdf_final = pd.concat(all_geometry, ignore_index=True)
    
    # Ensure all required fields are present (case-insensitive column check)
    out_fields_upper = [col.upper() for col in out_fields]
    col_map = {c.upper(): c for c in gdf_final.columns}
    
    # Select final columns including the geometry column (if it exists)
    final_cols = [col_map[c] for c in out_fields_upper if c in col_map]

    # Handle the geometry column name consistency
    if isinstance(gdf_final, gpd.GeoDataFrame):
        geom_name = gdf_final.geometry.name
        if geom_name != 'geometry':
            gdf_final = gdf_final.rename(columns={geom_name: 'geometry'})
            gdf_final.set_geometry('geometry', inplace=True)
            geom_name = 'geometry'
        if geom_name not in final_cols:
            final_cols.append(geom_name)
    
    # Use a set to maintain uniqueness and then convert back to a list
    return gdf_final[list(set(final_cols))].copy()

def fetch_all_meshblock_geometry(base_url: str) -> gpd.GeoDataFrame:
    """Fetches Meshblock Polygon geometry."""
    gdf_final = fetch_arcgis_geometry(base_url, 'MB_number', ['MB_number'])
    if not gdf_final.empty:
        # Standardize Meshblock ID to 7-digit string
        gdf_final['MB_number'] = gdf_final['MB_number'].astype(str).str.strip().str.zfill(7)
        print(f"✅ Successfully fetched total Meshblock geometry records: {len(gdf_final)}")
    return gdf_final

def fetch_and_clean_police_data(crime_url: str, meshblock_url: str) -> gpd.GeoDataFrame:
    """Downloads, merges, and filters crime data using Meshblock Polygon geometry."""
    print("--- 1. Processing Police Data ---")
    
    # ----------------------------------------------------
    # 1. Data Download and Initial Cleaning
    # ----------------------------------------------------
    print("   -> Downloading large crime data file...")
    try:
        crime_data_response = requests.get(crime_url)
        crime_data_response.raise_for_status()
        df_crime = pd.read_csv(io.BytesIO(crime_data_response.content), encoding='latin1')
        
        df_crime.columns = df_crime.columns.str.strip()
        df_crime.columns = [col.replace('ï»¿', '').strip() for col in df_crime.columns]
        
        CRIME_MONTH_COL_NAME = 'Year Month'
        if CRIME_MONTH_COL_NAME not in df_crime.columns: raise KeyError(f"Required '{CRIME_MONTH_COL_NAME}' column not found.")
            
        meshblock_cols = [col for col in df_crime.columns if 'meshblock' in col.lower()]
        if 'Meshblock' not in df_crime.columns and meshblock_cols:
            df_crime.rename(columns={meshblock_cols[0]: 'Meshblock'}, inplace=True)
        elif 'Meshblock' not in df_crime.columns:
            raise KeyError(f"Required 'Meshblock' column not found.")
        
        print(f"   -> Raw crime data records: {len(df_crime)}") 
        
    except Exception as e:
        print(f"❌ Failed to download or process crime data: {e}")
        raise
    
    # ----------------------------------------------------
    # 2. Fetch Geometry Data (Polygons Only)
    # ----------------------------------------------------
    gdf_meshblocks = fetch_all_meshblock_geometry(meshblock_url)
    
    if gdf_meshblocks.empty:
        return gpd.GeoDataFrame()
        
    # **FINAL ATTEMPT FIX**: Ensure Meshblock geometry column is consistently available as 'geometry' 
    # and rename it for the merge operation to prevent column name clashes.
    MESHBLOCK_GEOM_NAME = 'geometry_mb_source_temp'
    if 'geometry' in gdf_meshblocks.columns:
        gdf_meshblocks = gdf_meshblocks.rename(columns={'geometry': MESHBLOCK_GEOM_NAME})
    elif gdf_meshblocks.geometry.name in gdf_meshblocks.columns:
        # Fallback using the active geometry name
        gdf_meshblocks = gdf_meshblocks.rename(columns={gdf_meshblocks.geometry.name: MESHBLOCK_GEOM_NAME})
    else:
        # Critical failure to find geometry column
        raise KeyError("Meshblock geometry column not found in GeoDataFrame after fetch.")


    # Standardize Police data Meshblock ID (7-digit string)
    df_crime['Meshblock'] = df_crime['Meshblock'].astype(str).str.strip().str.zfill(7)
    
    # Filter for Auckland
    df_crime['Territorial Authority Cleaned'] = df_crime['Territorial Authority'].astype(str).apply(clean_territorial_authority)
    df_auckland = df_crime[df_crime['Territorial Authority Cleaned'].isin(AUCKLAND_AUTHORITIES_CLEANED)].copy()
    print(f"   -> Auckland filtered records: {len(df_auckland)}")
    
    # ----------------------------------------------------
    # 3. Merge: Meshblock Polygon Match
    # ----------------------------------------------------
    print("   -> Executing Merge: Meshblock Polygon geometry match...")

    df_merged = df_auckland.merge(
        gdf_meshblocks[['MB_number', MESHBLOCK_GEOM_NAME]], 
        left_on='Meshblock', 
        right_on='MB_number', 
        how='left'
    )
    # RENAME the source geometry column to the final expected name 'geometry'
    df_merged = df_merged.rename(columns={MESHBLOCK_GEOM_NAME: 'geometry'}) 
    
    unmatched_count = df_merged['geometry'].isna().sum()
    print(f"   -> Successfully matched records (Polygons): {len(df_merged) - unmatched_count}")
    
    # ----------------------------------------------------
    # 4. Data Cleaning and Final GeoDataFrame Creation
    # ----------------------------------------------------
    
    # CRITICAL FIX: Explicitly parse date format D/M/YYYY
    df_merged[CRIME_MONTH_COL_NAME] = pd.to_datetime(
        df_merged[CRIME_MONTH_COL_NAME], 
        format='%d/%m/%Y',  
        errors='coerce' 
    )
    
    df_final = df_merged.copy()

    df_final = df_final.rename(columns={
        'ANZSOC Division': 'OffenceType',     
        'Territorial Authority Cleaned': 'PoliceDistrict', 
        CRIME_MONTH_COL_NAME: 'CrimeMonth',
    })
    
    # Drop records that have no Polygon geometry
    initial_valid_count = len(df_final)
    df_final.dropna(subset=['geometry'], inplace=True) 
    
    print(f"✅ Police data processing complete. Final records (with Polygon geometry) for analysis: {len(df_final)}.")
    if len(df_final) < initial_valid_count:
        print(f"⚠️ Note: {initial_valid_count - len(df_final)} records dropped due to lack of Meshblock Polygon geometry.")
    
    gdf_crime = gpd.GeoDataFrame(
        df_final,
        geometry='geometry', 
        crs="EPSG:4326"
    )
    
    final_cols = ['OffenceType', 'PoliceDistrict', 'CrimeMonth', 'geometry', 'Meshblock']
    return gdf_crime[[col for col in final_cols if col in gdf_crime.columns]]


# --- 3. Fetch Route Geometry ---

def fetch_route_geometry() -> gpd.GeoDataFrame:
    """Fetches bus route geometry (LineString)."""
    print("--- 2. Fetching AT Route Geometry (LineString) ---")
    try:
        route_response = requests.get(ARCGIS_ROUTES_URL)
        route_response.raise_for_status() 
        gdf_routes = gpd.read_file(io.BytesIO(route_response.content))
        
        gdf_routes.rename(columns={'ROUTENUMBER': 'Route No'}, inplace=True) 
        gdf_routes = gdf_routes[gdf_routes['MODE'] == 'Bus'].copy()
        gdf_routes = gdf_routes[['Route No', 'geometry']].copy()
        gdf_routes['Route No'] = gdf_routes['Route No'].astype(str)
        
        print(f"✅ Successfully fetched {len(gdf_routes)} bus route geometries.")
        return gdf_routes
    except Exception as e:
        print(f"❌ Failed to fetch ArcGIS Route data: {e}")
        raise

def fetch_stop_geometry() -> gpd.GeoDataFrame:
    """Fetches bus stop geometry (Point)."""
    print("--- 3. Fetching AT Bus Stop Geometry (Point) ---")
    
    out_fields = ['STOPID', 'STOPNAME', 'MODE']
    
    # fetch_arcgis_geometry now returns the data in EPSG:2193 (NZTM2000)
    gdf_stops = fetch_arcgis_geometry(ARCGIS_STOPS_URL, 'STOPID', out_fields, mode='json')
    
    if gdf_stops.empty:
        print("⚠️ Warning: Failed to retrieve bus stop data.")
        return gdf_stops
        
    gdf_stops.columns = [col.upper() for col in gdf_stops.columns] # Normalize column names
    gdf_stops = gdf_stops[gdf_stops['MODE'] == 'Bus'].copy()
    
    # Transform stops back to EPSG:4326 for consistency before analyze_and_aggregate
    gdf_stops = gdf_stops.to_crs(epsg=4326)
    
    print(f"✅ Successfully fetched and aligned {len(gdf_stops)} bus stop geometries.")
    return gdf_stops[['STOPID', 'geometry']]


# --- 4. Spatial Analysis and Aggregation ---

def analyze_and_aggregate(gdf_routes: gpd.GeoDataFrame, gdf_crime: gpd.GeoDataFrame, gdf_stops: gpd.GeoDataFrame):
    """Performs spatial join using Polygon-Line Intersection and Polygon-Point Containment."""
    print("--- 4. Executing Spatial Analysis and Aggregation ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True) 
    
    if gdf_crime.empty:
        print("⚠️ Warning: Skipping spatial analysis due to no valid Auckland crime data.")
        min_date = 'N/A'
        max_date = 'N/A'
        empty_geojson_output(gdf_routes) 
        empty_stats_output(min_date, max_date)
        return

    # --- 1. Align CRS for Spatial Operations ---
    
    # Project Routes (LineString) to TARGET_CRS (NZTM2000)
    gdf_routes_proj = gdf_routes.to_crs(TARGET_CRS).reset_index(names=['route_geom_id']) 
    
    # Project Stops (Point) to TARGET_CRS
    gdf_stops_proj = gdf_stops.to_crs(TARGET_CRS)
    
    # Project Crime (Polygon) to TARGET_CRS
    if gdf_crime.crs is None or gdf_crime.crs != "EPSG:4326":
        gdf_crime.set_crs(epsg=4326, inplace=True)
        
    gdf_crime_proj = gdf_crime.to_crs(TARGET_CRS).reset_index(names=['crime_data_id'])
    print(f"   -> Successfully aligned CRS for all datasets to {TARGET_CRS}.")
    
    # -------------------------------------------------------------------------
    # 2. Association Method 1: Crime Polygon intersects Route Line
    # -------------------------------------------------------------------------
    print("   -> 2.1 Performing Line-Polygon intersection join...")
    
    line_join = gpd.sjoin(
        gdf_crime_proj[['crime_data_id', 'Meshblock', 'geometry']], 
        gdf_routes_proj[['Route No', 'route_geom_id', 'geometry']], 
        how='inner', 
        predicate='intersects'
    )
    
    # -------------------------------------------------------------------------
    # 3. Association Method 2: Crime Polygon contains Bus Stop Point
    # -------------------------------------------------------------------------
    
    # Join crime polygons (left) with bus stop points (right)
    print("   -> 2.2 Performing Polygon-Point containment join (Meshblock contains Stop)...")
    stop_join = gpd.sjoin(
        gdf_crime_proj[['crime_data_id', 'Meshblock', 'geometry']],
        gdf_stops_proj[['STOPID', 'geometry']],
        how='inner',
        predicate='contains'
    )

    # Find all unique Meshblocks that contain a stop AND have a crime
    crime_meshblocks_with_stops = stop_join[['Meshblock']].drop_duplicates()
    
    # Identify which routes pass through these Meshblocks (using the initial line_join)
    stop_route_join = line_join[['crime_data_id', 'Meshblock', 'route_geom_id', 'Route No']].merge(
        crime_meshblocks_with_stops, 
        on='Meshblock', 
        how='inner'
    ).drop_duplicates(subset=['crime_data_id', 'Route No'])
    
    # -------------------------------------------------------------------------
    # 4. Combine and Aggregate Results
    # -------------------------------------------------------------------------
    
    # Combine results from both methods
    line_join = line_join[['crime_data_id', 'Meshblock', 'route_geom_id', 'Route No']].drop_duplicates()
    
    combined_crime_routes = pd.concat([line_join, stop_route_join]).drop_duplicates(subset=['crime_data_id', 'Route No'])
    
    print(f"   -> Final unique Crime-Route associations: {len(combined_crime_routes)}") 
    
    # Merge crime data attributes back to the combined association table
    crime_data_attributes = gdf_crime_proj.drop(columns=['geometry']).set_index('crime_data_id')[['OffenceType', 'CrimeMonth']].copy()
    crime_counts = combined_crime_routes.merge(crime_data_attributes, left_on='crime_data_id', right_index=True, how='left')

    if crime_counts.empty:
        print("❌ CRITICAL: The spatial join returned zero records after both Line and Stop checks.")
        BUFFER_DISTANCE_M = '0 (Polygon-Line/Polygon-Point)'
        min_date = 'N/A'
        max_date = 'N/A'
        empty_geojson_output(gdf_routes) 
        empty_stats_output(min_date, max_date)
        return
    else:
        BUFFER_DISTANCE_M = '0 (Polygon-Line/Polygon-Point)'
        valid_crime_months = crime_counts['CrimeMonth'].dropna()
        if not valid_crime_months.empty:
            min_date = valid_crime_months.min().strftime('%Y-%m-%d')
            max_date = valid_crime_months.max().strftime('%Y-%m-%d')
        else:
            min_date = 'N/A (All dates invalid)'
            max_date = 'N/A (All dates invalid)'

    # 5. Aggregate total crime per route (using 'route_geom_id')
    total_crime_summary = crime_counts.groupby('route_geom_id').size().reset_index(name='Total_Crime_Count')
    
    # 6. Aggregate crime details (trend and type)
    crime_details = {
        'metadata': {
            'crime_period_start': min_date,
            'crime_period_end': max_date,
            'buffer_distance_m': BUFFER_DISTANCE_M, 
            'data_source': 'NZ Police (Polygon Intersection) + AT Bus Lines/Stops'
        },
        'routes': {}
    }
    
    for route_id in total_crime_summary['route_geom_id'].unique():
        route_data = crime_counts[crime_counts['route_geom_id'] == route_id]
        route_no = gdf_routes_proj[gdf_routes_proj['route_geom_id'] == route_id]['Route No'].iloc[0]
        
        valid_dates = route_data.dropna(subset=['CrimeMonth'])
        monthly_trend = valid_dates.groupby(valid_dates['CrimeMonth'].dt.to_period('M')).size().to_dict()
        monthly_trend = {str(k): int(v) for k, v in monthly_trend.items()}
        
        type_breakdown = route_data['OffenceType'].value_counts().to_dict()
        type_breakdown = {k: int(v) for k, v in type_breakdown.items()}
        
        crime_details['routes'][route_no] = {
            'monthly_trend': monthly_trend,
            'type_breakdown': type_breakdown
        }

    # 7. Merge total crime count back to route GeoDataFrame
    gdf_results = gdf_routes_proj.merge(total_crime_summary, 
                                        on='route_geom_id', 
                                        how='left')
    gdf_results['Total_Crime_Count'] = gdf_results['Total_Crime_Count'].fillna(0).astype(int)
    
    # Final output CRS should be EPSG:4326 for web display
    gdf_output = gdf_results.to_crs(epsg=4326)[['Route No', 'Total_Crime_Count', 'geometry']].copy()

    # 8. Save results
    gdf_output.to_file(OUTPUT_FILE, driver='GeoJSON', encoding='utf-8')
    print(f"✅ GeoJSON output to {OUTPUT_FILE}")
    
    with open(STATS_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(crime_details, f, ensure_ascii=False, indent=4)
    print(f"✅ Crime breakdown statistics output to {STATS_OUTPUT_FILE}")

def empty_geojson_output(gdf_routes):
    # Ensure routes are in 4326 for output
    if gdf_routes.crs != 'EPSG:4326':
        gdf_routes = gdf_routes.to_crs(epsg=4326)
        
    gdf_routes['Total_Crime_Count'] = 0
    gdf_routes = gdf_routes[['Route No', 'Total_Crime_Count', 'geometry']].copy()
    gdf_routes.to_file(OUTPUT_FILE, driver='GeoJSON', encoding='utf-8')

def empty_stats_output(min_date, max_date):
    crime_details = {
        'metadata': {
            'crime_period_start': min_date,
            'crime_period_end': max_date,
            'buffer_distance_m': '0 (Polygon-Line/Polygon-Point)',
            'data_source': 'NZ Police (Polygon Intersection) + AT Bus Lines/Stops'
        },
        'routes': {}
    }
    with open(STATS_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(crime_details, f, ensure_ascii=False, indent=4)


# --- 5. Main Flow ---
def run_etl():
    """Runs the ETL pipeline."""
    if not POLICE_DATA_URL:
        print("❌ Error: POLICE_DATA_URL environment variable is missing. Please set it in GitHub Secrets.")
        sys.exit(1)
        
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 1. Fetch Crime (Polygons)
        gdf_crime = fetch_and_clean_police_data(POLICE_DATA_URL, MESHBLOCK_BASE_URL) 
        
        # 2. Fetch Route Lines
        gdf_routes = fetch_route_geometry()
        
        # 3. Fetch Bus Stops (Points)
        gdf_stops = fetch_stop_geometry()
        
        # 4. Analyze
        analyze_and_aggregate(gdf_routes, gdf_crime, gdf_stops)
        print("\n🎉 ETL pipeline completed successfully!")
    except Exception as e:
        error_message = str(e).strip()
        print(f"\n❌ ETL pipeline interrupted: {error_message}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
