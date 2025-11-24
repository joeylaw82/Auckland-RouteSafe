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
# Meshblock Backup URL (Hugging Face CSV for point geometry)
MESHBLOCK_BACKUP_URL = "https://huggingface.co/datasets/JoeyBBBBBB/VictimisationTimeAndPlaceAugst2021ToSeptember2025/resolve/main/Map%20sheet_data.csv"
ARCGIS_ROUTES_URL = "https://services2.arcgis.com/JkPEgZJGxhSjYOo0/arcgis/rest/services/BusService/FeatureServer/2/query?where=1%3D1&outFields=*&f=geojson"

AUCKLAND_AUTHORITIES = ['Auckland','Waitemata', 'Counties Manukau', 'Franklin', 'Auckland City'] 

# Output file paths
OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'route_crime_stats.geojson')
STATS_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'crime_breakdown.json')

# Max records per ArcGIS request for stability
MAX_RECORDS = 500 


# --- 2. Helper Functions ---

def clean_territorial_authority(name: str) -> str:
    """Cleans up the territorial authority name."""
    if pd.isna(name): return ''
    cleaned = re.sub(r'[^\w\s]', '', str(name), flags=re.UNICODE) 
    cleaned = re.sub(r'\s+', ' ', cleaned).strip() 
    return cleaned.upper()

AUCKLAND_AUTHORITIES_CLEANED = [clean_territorial_authority(name) for name in AUCKLAND_AUTHORITIES]


def fetch_arcgis_geometry(base_url: str, id_field: str, out_fields: list) -> gpd.GeoDataFrame:
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
            'f': 'geojson',
            'inSR': '4326', 
            'outSR': '4326',
        }
        
        query_url = f"{base_url}/query?{urlencode(query_params)}"
        
        try:
            response = requests.get(query_url)
            response.raise_for_status()
            
            gdf_batch = gpd.read_file(io.BytesIO(response.content))
            
            if gdf_batch.empty:
                print("   -> 🚨 Warning: ArcGIS service returned an empty batch. Stopping fetch.")
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
    gdf_final = gdf_final[out_fields + ['geometry']].copy()
    
    return gdf_final

def fetch_all_meshblock_geometry(base_url: str) -> gpd.GeoDataFrame:
    """Fetches Meshblock Polygon geometry."""
    gdf_final = fetch_arcgis_geometry(base_url, 'MB_number', ['MB_number'])
    if not gdf_final.empty:
        # Standardize Meshblock ID to 7-digit string
        gdf_final['MB_number'] = gdf_final['MB_number'].astype(str).str.strip().str.zfill(7)
        print(f"✅ Successfully fetched total Meshblock geometry records: {len(gdf_final)}")
    return gdf_final

def fetch_meshblock_backup_points(backup_url: str) -> gpd.GeoDataFrame:
    """Downloads the backup CSV, extracts Meshblock ID and coordinates, and creates Point geometry."""
    print("   -> Downloading Meshblock backup CSV and creating point geometry...")
    try:
        response = requests.get(backup_url)
        response.raise_for_status()
        df_backup = pd.read_csv(io.BytesIO(response.content), encoding='latin1')
        
        df_backup.columns = df_backup.columns.str.strip()
        df_backup.columns = [col.replace('ï»¿', '').strip() for col in df_backup.columns]

        # Explicit column names based on user sample
        MESHBLOCK_ID_COL = 'Map Detail Name'
        LAT_COL = 'Latitude (generated)'
        LON_COL = 'Longitude (generated)'
        
        if not all(col in df_backup.columns for col in [MESHBLOCK_ID_COL, LAT_COL, LON_COL]):
             print("❌ Backup CSV missing required columns. Skipping backup geometry.")
             return gpd.GeoDataFrame()
             
        df_backup.rename(columns={MESHBLOCK_ID_COL: 'Meshblock'}, inplace=True)
        
        # Standardize Meshblock ID to 7-digit string
        df_backup['Meshblock'] = df_backup['Meshblock'].astype(str).str.strip().str.zfill(7)
        
        # Create Point geometry
        df_backup.dropna(subset=['Meshblock', LAT_COL, LON_COL], inplace=True)
        df_backup[LON_COL] = pd.to_numeric(df_backup[LON_COL], errors='coerce')
        df_backup[LAT_COL] = pd.to_numeric(df_backup[LAT_COL], errors='coerce')
        df_backup.dropna(subset=[LON_COL, LAT_COL], inplace=True)
        
        geometry = [Point(xy) for xy in zip(df_backup[LON_COL], df_backup[LAT_COL])]
        
        gdf_points = gpd.GeoDataFrame(df_backup[['Meshblock']].copy(), geometry=geometry, crs="EPSG:4326")
        
        print(f"✅ Successfully created {len(gdf_points)} Meshblock points from backup CSV.")
        return gdf_points[['Meshblock', 'geometry']]

    except Exception as e:
        print(f"❌ Failed to process Meshblock backup CSV: {e}")
        return gpd.GeoDataFrame()


def fetch_and_clean_police_data(crime_url: str, meshblock_url: str, backup_url: str) -> gpd.GeoDataFrame:
    """Downloads, merges, and filters crime data (Meshblock Polygon + Meshblock Point backup matching)."""
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
    # 2. Fetch Geometry Data
    # ----------------------------------------------------
    gdf_meshblocks = fetch_all_meshblock_geometry(meshblock_url)
    gdf_backup_points = fetch_meshblock_backup_points(backup_url) 
    
    if gdf_meshblocks.empty and gdf_backup_points.empty:
        return gpd.GeoDataFrame()

    # Standardize Police data Meshblock ID (7-digit string)
    df_crime['Meshblock'] = df_crime['Meshblock'].astype(str).str.strip().str.zfill(7)
    
    # Filter for Auckland
    df_crime['Territorial Authority Cleaned'] = df_crime['Territorial Authority'].astype(str).apply(clean_territorial_authority)
    df_auckland = df_crime[df_crime['Territorial Authority Cleaned'].isin(AUCKLAND_AUTHORITIES_CLEANED)].copy()
    print(f"   -> Auckland filtered records: {len(df_auckland)}")
    
    # ----------------------------------------------------
    # 3. Phase 1: Meshblock Polygon Match
    # ----------------------------------------------------
    print("   -> Executing Phase 1: Meshblock Polygon geometry match...")
    df_merged = df_auckland.merge(
        gdf_meshblocks[['MB_number', 'geometry']], 
        left_on='Meshblock', 
        right_on='MB_number', 
        how='left'
    )
    df_merged = df_merged.rename(columns={'geometry': 'geometry_mb'})
    
    unmatched_count_1 = df_merged['geometry_mb'].isna().sum()
    print(f"   -> Phase 1: Successfully matched records: {len(df_merged) - unmatched_count_1}")
    print(f"   -> Phase 1: Unmatched records: {unmatched_count_1}")
    
    if unmatched_count_1 > 0 and not gdf_backup_points.empty:
        # ----------------------------------------------------
        # 4. Phase 2: Meshblock Point Match (for unmatched records)
        # ----------------------------------------------------
        print("   -> Executing Phase 2: Attempting Meshblock backup point geometry match...")
        
        df_unmatched = df_merged[df_merged['geometry_mb'].isna()].copy()
        
        df_point_merged = df_unmatched.merge(
            gdf_backup_points[['Meshblock', 'geometry']],
            left_on='Meshblock',
            right_on='Meshblock',
            how='left'
        )
        df_point_merged = df_point_merged.rename(columns={'geometry': 'geometry_point'})
        
        # Fill missing Polygon geometry with Point geometry
        df_merged.loc[df_merged['geometry_mb'].isna(), 'geometry_mb'] = df_point_merged['geometry_point'].values
        df_merged = df_merged.rename(columns={'geometry_mb': 'geometry'})
        
        unmatched_count_2 = df_merged['geometry'].isna().sum()
        print(f"   -> Phase 2: Still unmatched records: {unmatched_count_2}")
        print(f"   -> Total successfully matched records: {len(df_merged) - unmatched_count_2}")
    else:
        df_merged = df_merged.rename(columns={'geometry_mb': 'geometry'})
        unmatched_count_2 = unmatched_count_1

    # ----------------------------------------------------
    # 5. Data Cleaning and Debug Output
    # ----------------------------------------------------
    
    # CRITICAL FIX: Explicitly parse date format D/M/YYYY
    print("   -> Converting date format (using %d/%m/%Y)...")
    df_merged[CRIME_MONTH_COL_NAME] = pd.to_datetime(
        df_merged[CRIME_MONTH_COL_NAME], 
        format='%d/%m/%Y',  
        errors='coerce' 
    )
    
    df_final = df_merged.copy()

    df_final = df_final.rename(columns={
        'ANZSOC Division': 'OffenceType',     
        'Territorial Authority Cleaned': 'PoliceDistrict', 
        CRIME_MONTH_COL_NAME: 'CrimeMonth'
    })
    
    # Debug CSV output
    DEBUG_CSV_FILE = os.path.join(OUTPUT_DIR, 'auckland_crime_debug.csv')
    cols_to_drop = ['geometry', 'MB_number', 'Territorial Authority']
    cols_to_drop = [col for col in cols_to_drop if col in df_final.columns]
    
    df_final.drop(columns=cols_to_drop, errors='ignore').to_csv(DEBUG_CSV_FILE, index=False, encoding='utf-8') 
    print(f"✅ Debug file (auckland_crime_debug.csv) output to {DEBUG_CSV_FILE}")

    # Check and drop invalid rows
    missing_geometry_count = df_final['geometry'].isna().sum()
    print(f"   -> 🚨 Check: Records missing geometry after two phases: {missing_geometry_count}")
    
    # CRITICAL FIX: Only drop based on 'geometry' as requested
    initial_valid_count = len(df_final)
    df_final.dropna(subset=['geometry'], inplace=True) 
    
    print(f"✅ Police data processing complete. Final records for analysis: {len(df_final)}.")
    if len(df_final) < initial_valid_count:
        print(f"⚠️ Note: {initial_valid_count - len(df_final)} records dropped due to lack of geometry.")
    
    gdf_crime = gpd.GeoDataFrame(
        df_final,
        geometry='geometry', 
        crs="EPSG:4326"
    )
    
    final_cols = ['OffenceType', 'PoliceDistrict', 'CrimeMonth', 'geometry']
    return gdf_crime[[col for col in final_cols if col in gdf_crime.columns]]


# --- 3. Fetch Route Geometry ---
def fetch_route_geometry() -> gpd.GeoDataFrame:
    """Fetches bus route geometry data."""
    print("--- 2. Fetching AT Route Geometry ---")
    try:
        arcgis_response = requests.get(ARCGIS_ROUTES_URL)
        arcgis_response.raise_for_status() 
        gdf_routes = gpd.read_file(io.BytesIO(arcgis_response.content))
        
        gdf_routes.rename(columns={'ROUTENUMBER': 'Route No'}, inplace=True) 
        gdf_routes = gdf_routes[gdf_routes['MODE'] == 'Bus'].copy()
        gdf_routes = gdf_routes[['Route No', 'geometry']].copy()
        gdf_routes['Route No'] = gdf_routes['Route No'].astype(str)
        
        print(f"✅ Successfully fetched {len(gdf_routes)} bus route geometries.")
        return gdf_routes
    except Exception as e:
        print(f"❌ Failed to fetch ArcGIS data: {e}")
        raise


# --- 4. Spatial Analysis and Aggregation ---

def analyze_and_aggregate(gdf_routes: gpd.GeoDataFrame, gdf_crime: gpd.GeoDataFrame):
    """Performs spatial join, calculates statistics, and generates GeoJSON and JSON files."""
    print("--- 3. Executing Spatial Analysis and Aggregation ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True) 
    
    if gdf_crime.empty:
        print("⚠️ Warning: Skipping spatial analysis due to no valid Auckland crime data.")
        min_date = 'N/A'
        max_date = 'N/A'
        empty_geojson_output(gdf_routes) 
        empty_stats_output(min_date, max_date)
        return

    # 1. Create 50m buffer
    gdf_routes_proj = gdf_routes.to_crs(epsg=2193) 
    gdf_routes_buffer = gdf_routes_proj.copy()
    gdf_routes_buffer['geometry'] = gdf_routes_buffer.geometry.buffer(50) 
    
    # 2. Project crime data
    gdf_crime_proj = gdf_crime.to_crs(epsg=2193)
    
    # 3. Spatial Join
    crime_counts = gpd.sjoin(gdf_crime_proj, gdf_routes_buffer.reset_index(), how='inner', predicate='intersects')
    
    print(f"   -> Crime incidents after spatial join: {len(crime_counts)}") 

    if crime_counts.empty:
        print("⚠️ Warning: No crime incidents fell within the 50m buffer of any bus route.")
        min_date = 'N/A'
        max_date = 'N/A'
    else:
        valid_crime_months = crime_counts['CrimeMonth'].dropna()
        if not valid_crime_months.empty:
            min_date = valid_crime_months.min().strftime('%Y-%m-%d')
            max_date = valid_crime_months.max().strftime('%Y-%m-%d')
        else:
            min_date = 'N/A (All dates invalid)'
            max_date = 'N/A (All dates invalid)'

    # 5. Aggregate total crime per route
    total_crime_summary = crime_counts.groupby('index_right').size().reset_index(name='Total_Crime_Count')
    
    # 6. Aggregate crime details (trend and type)
    crime_details = {
        'metadata': {
            'crime_period_start': min_date,
            'crime_period_end': max_date,
            'buffer_distance_m': 50,
            'data_source': 'NZ Police (Full Available Dataset) merged with NZ Meshblock/Backup Point Geometry'
        },
        'routes': {}
    }
    
    for route_index in total_crime_summary['index_right'].unique():
        route_data = crime_counts[crime_counts['index_right'] == route_index]
        route_no = gdf_routes_buffer.loc[route_index, 'Route No']
        
        # Trend: only group non-NaT dates
        valid_dates = route_data.dropna(subset=['CrimeMonth'])
        monthly_trend = valid_dates.groupby(valid_dates['CrimeMonth'].dt.to_period('M')).size().to_dict()
        monthly_trend = {str(k): int(v) for k, v in monthly_trend.items()}
        
        # Breakdown: value_counts handles NaNs by default (excluding them)
        type_breakdown = route_data['OffenceType'].value_counts().to_dict()
        type_breakdown = {k: int(v) for k, v in type_breakdown.items()}
        
        crime_details['routes'][route_no] = {
            'monthly_trend': monthly_trend,
            'type_breakdown': type_breakdown
        }

    # 7. Merge total crime count back to route GeoDataFrame
    gdf_results = gdf_routes_buffer.reset_index().merge(total_crime_summary, 
                                                        left_on='index', 
                                                        right_on='index_right', 
                                                        how='left')
    gdf_results['Total_Crime_Count'] = gdf_results['Total_Crime_Count'].fillna(0).astype(int)
    gdf_output = gdf_results.to_crs(epsg=4326)[['Route No', 'Total_Crime_Count', 'geometry']].copy()

    # 8. Save results
    gdf_output.to_file(OUTPUT_FILE, driver='GeoJSON', encoding='utf-8')
    print(f"✅ GeoJSON output to {OUTPUT_FILE}")
    
    with open(STATS_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(crime_details, f, ensure_ascii=False, indent=4)
    print(f"✅ Crime breakdown statistics output to {STATS_OUTPUT_FILE}")

def empty_geojson_output(gdf_routes):
    gdf_routes['Total_Crime_Count'] = 0
    gdf_routes = gdf_routes.to_crs(epsg=4326)[['Route No', 'Total_Crime_Count', 'geometry']].copy()
    gdf_routes.to_file(OUTPUT_FILE, driver='GeoJSON', encoding='utf-8')

def empty_stats_output(min_date, max_date):
    crime_details = {
        'metadata': {
            'crime_period_start': min_date,
            'crime_period_end': max_date,
            'buffer_distance_m': 50,
            'data_source': 'NZ Police (Full Available Dataset) merged with NZ Meshblock/Backup Point Geometry'
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
        
        gdf_crime = fetch_and_clean_police_data(POLICE_DATA_URL, MESHBLOCK_BASE_URL, MESHBLOCK_BACKUP_URL) 
        gdf_routes = fetch_route_geometry()
        analyze_and_aggregate(gdf_routes, gdf_crime)
        print("\n🎉 ETL pipeline completed successfully!")
    except Exception as e:
        error_message = str(e).strip()
        print(f"\n❌ ETL pipeline interrupted: {error_message}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
