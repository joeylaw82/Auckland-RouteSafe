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

# --- 1. 配置區 (Configuration) ---
POLICE_DATA_URL = os.environ.get("POLICE_DATA_URL") 
MESHBLOCK_BASE_URL = "https://services.arcgis.com/XTtANUDT8Va4DLwI/arcgis/rest/services/nz_meshblocks/FeatureServer/0"
# 新增的 Area Unit URL
AREA_UNIT_BASE_URL = "https://services2.arcgis.com/vKb0s8tBIA3bdocZ/ArcGIS/rest/services/Area_Unit_2017/FeatureServer/0"
ARCGIS_ROUTES_URL = "https://services2.arcgis.com/JkPEgZJGxhSjYOo0/arcgis/rest/services/BusService/FeatureServer/2/query?where=1%3D1&outFields=*&f=geojson"

AUCKLAND_AUTHORITIES = ['Auckland','Waitemata', 'Counties Manukau', 'Franklin', 'Auckland City'] 

# 輸出文件路徑
OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'route_crime_stats.geojson')
STATS_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'crime_breakdown.json')
DEBUG_CSV_FILE = os.path.join(OUTPUT_DIR, 'auckland_crime_debug.csv') 

MAX_RECORDS = 2000 # ArcGIS 服務的單次查詢記錄限制


# --- 2. 輔助函數 (Helper Functions) ---

def clean_territorial_authority(name: str) -> str:
    """清理行政區名稱。"""
    if pd.isna(name): return ''
    cleaned = re.sub(r'[^\w\s]', '', str(name), flags=re.UNICODE) 
    cleaned = re.sub(r'\s+', ' ', cleaned).strip() 
    return cleaned.upper()

AUCKLAND_AUTHORITIES_CLEANED = [clean_territorial_authority(name) for name in AUCKLAND_AUTHORITIES]


def fetch_arcgis_geometry(base_url: str, id_field: str, out_fields: list) -> gpd.GeoDataFrame:
    """通用的 ArcGIS 分頁獲取幾何函數。"""
    print(f"   -> 正在使用分頁技術獲取 {id_field} 幾何...")
    
    out_fields_str = ','.join(out_fields)
    count_url = f"{base_url}/query?where=1%3D1&returnCountOnly=true&f=json"
    
    try:
        count_response = requests.get(count_url)
        count_response.raise_for_status()
        total_count = count_response.json().get('count', 0)
        print(f"   -> 服務報告總記錄數: {total_count}")
        if total_count == 0:
            print(f"❌ 錯誤: ArcGIS 服務報告 {id_field} 總記錄數為零。")
            return gpd.GeoDataFrame()
    except Exception as e:
        print(f"❌ 獲取 {id_field} 總記錄數失敗: {e}")
        return gpd.GeoDataFrame()

    all_geometry = []
    offset = 0
    
    while offset < total_count:
        print(f"   -> 正在獲取批次：記錄 {offset} 到 {min(offset + MAX_RECORDS, total_count)}...")
        
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
                print("   -> 🚨 警告：ArcGIS 服務返回空批次。停止獲取。")
                break
                
            all_geometry.append(gdf_batch)
            offset += len(gdf_batch)
            sleep(0.5) 
            
        except Exception as e:
            print(f"❌ 獲取批次數據失敗 (Offset: {offset}): {e}")
            break
            
    if not all_geometry:
        print(f"❌ 錯誤：未能獲取任何 {id_field} 數據。")
        return gpd.GeoDataFrame()
        
    gdf_final = pd.concat(all_geometry, ignore_index=True)
    gdf_final = gdf_final[out_fields + ['geometry']].copy()
    
    return gdf_final

def fetch_all_meshblock_geometry(base_url: str) -> gpd.GeoDataFrame:
    """獲取 Meshblock 幾何。"""
    gdf_final = fetch_arcgis_geometry(base_url, 'MB_number', ['MB_number'])
    if not gdf_final.empty:
        # 標準化 MB_number 為 7 位字串
        gdf_final['MB_number'] = gdf_final['MB_number'].astype(str).str.strip().str.zfill(7)
        print(f"✅ 成功獲取所有 Meshblock 幾何總記錄數: {len(gdf_final)}")
    return gdf_final

def fetch_all_area_unit_geometry(base_url: str) -> gpd.GeoDataFrame:
    """獲取 Area Unit 幾何。"""
    out_fields = ['AU2017_V1_00', 'AU2017_V1_00_NAME']
    gdf_final = fetch_arcgis_geometry(base_url, 'AU2017_V1_00', out_fields)
    if not gdf_final.empty:
        # Area Unit Code 通常是 6 位，我們將其標準化
        gdf_final['AU_code'] = gdf_final['AU2017_V1_00'].astype(str).str.strip().str.zfill(6)
        gdf_final = gdf_final.rename(columns={'AU2017_V1_00_NAME': 'Area Unit Name'})
        print(f"✅ 成功獲取所有 Area Unit 幾何總記錄數: {len(gdf_final)}")
    return gdf_final


def fetch_and_clean_police_data(crime_url: str, meshblock_url: str, area_unit_url: str) -> gpd.GeoDataFrame:
    """下載、合併和篩選犯罪數據 (包含兩階段幾何匹配)。"""
    print("--- 1. 正在處理警察數據 ---")
    
    # ----------------------------------------------------
    # 1. 數據下載和初始清理 (與之前相同)
    # ----------------------------------------------------
    print("   -> 正在下載大型犯罪數據文件...")
    try:
        crime_data_response = requests.get(crime_url)
        crime_data_response.raise_for_status()
        df_crime = pd.read_csv(io.BytesIO(crime_data_response.content), encoding='latin1')
        
        # 核心欄位清理
        df_crime.columns = df_crime.columns.str.strip()
        df_crime.columns = [col.replace('ï»¿', '').strip() for col in df_crime.columns]
        
        CRIME_MONTH_COL_NAME = 'Year Month'
        if CRIME_MONTH_COL_NAME not in df_crime.columns: raise KeyError(f"找不到必要的 '{CRIME_MONTH_COL_NAME}' 欄位。")
            
        meshblock_cols = [col for col in df_crime.columns if 'meshblock' in col.lower()]
        if 'Meshblock' not in df_crime.columns and meshblock_cols:
            df_crime.rename(columns={meshblock_cols[0]: 'Meshblock'}, inplace=True)
        elif 'Meshblock' not in df_crime.columns:
            raise KeyError(f"找不到必要的 'Meshblock' 欄位。")
        
        print(f"   -> 犯罪數據原始記錄數: {len(df_crime)}") 
        
    except Exception as e:
        print(f"❌ 下載或處理犯罪數據失敗: {e}")
        raise
    
    # ----------------------------------------------------
    # 2. 獲取所有幾何數據
    # ----------------------------------------------------
    gdf_meshblocks = fetch_all_meshblock_geometry(meshblock_url)
    gdf_area_units = fetch_all_area_unit_geometry(area_unit_url)
    
    if gdf_meshblocks.empty and gdf_area_units.empty:
        return gpd.GeoDataFrame()

    # 標準化警察數據的 Meshblock ID (7 位字串)
    df_crime['Meshblock'] = df_crime['Meshblock'].astype(str).str.strip().str.zfill(7)
    
    # 應用 TA 清理函數並篩選奧克蘭
    df_crime['Territorial Authority Cleaned'] = df_crime['Territorial Authority'].astype(str).apply(clean_territorial_authority)
    df_auckland = df_crime[df_crime['Territorial Authority Cleaned'].isin(AUCKLAND_AUTHORITIES_CLEANED)].copy()
    print(f"   -> 奧克蘭TA過濾後記錄數: {len(df_auckland)}")
    
    # ----------------------------------------------------
    # 3. 階段一：Meshblock 匹配 (優先匹配)
    # ----------------------------------------------------
    print("   -> 執行階段一：Meshblock 幾何匹配...")
    df_merged = df_auckland.merge(
        gdf_meshblocks[['MB_number', 'geometry']], 
        left_on='Meshblock', 
        right_on='MB_number', 
        how='left'
    )
    df_merged = df_merged.rename(columns={'geometry': 'geometry_mb'})
    
    unmatched_count_1 = df_merged['geometry_mb'].isna().sum()
    print(f"   -> 階段一：成功匹配記錄數: {len(df_merged) - unmatched_count_1}")
    print(f"   -> 階段一：未匹配記錄數: {unmatched_count_1}")
    
    if unmatched_count_1 > 0 and not gdf_area_units.empty:
        # ----------------------------------------------------
        # 4. 階段二：Area Unit 匹配 (針對未匹配的記錄)
        # ----------------------------------------------------
        print("   -> 執行階段二：嘗試使用 Area Unit 幾何匹配未匹配的記錄...")
        
        # 提取未匹配的行
        df_unmatched = df_merged[df_merged['geometry_mb'].isna()].copy()
        
        # 假設 Area Unit Code 的格式與 Meshblock Code 相似，但長度為 6
        # 我們截斷 Meshblock ID，並嘗試將其視為 Area Unit Code (AU2017_V1_00)
        df_unmatched['AU_code_match'] = df_unmatched['Meshblock'].str[:6]
        
        df_area_merged = df_unmatched.merge(
            gdf_area_units[['AU_code', 'geometry']],
            left_on='AU_code_match',
            right_on='AU_code',
            how='left'
        )
        df_area_merged = df_area_merged.rename(columns={'geometry': 'geometry_au'})
        
        # 填充主數據框
        df_merged.loc[df_merged['geometry_mb'].isna(), 'geometry_mb'] = df_area_merged['geometry_au'].values
        df_merged = df_merged.rename(columns={'geometry_mb': 'geometry'}) # 最終使用的幾何欄位
        
        unmatched_count_2 = df_merged['geometry'].isna().sum()
        print(f"   -> 階段二：再次未匹配記錄數: {unmatched_count_2}")
        print(f"   -> 總匹配成功記錄數: {len(df_merged) - unmatched_count_2}")
    else:
        df_merged = df_merged.rename(columns={'geometry_mb': 'geometry'}) # 如果沒有第二階段，直接重命名
        unmatched_count_2 = unmatched_count_1

    # ----------------------------------------------------
    # 5. 數據清理和除錯輸出
    # ----------------------------------------------------
    
    # 轉換時間欄位
    df_merged[CRIME_MONTH_COL_NAME] = pd.to_datetime(
        df_merged[CRIME_MONTH_COL_NAME], 
        format='%Y-%m-%d', 
        errors='coerce' 
    )
    
    df_final = df_merged.copy()

    df_final = df_final.rename(columns={
        'ANZSOC Division': 'OffenceType',     
        'Territorial Authority Cleaned': 'PoliceDistrict', 
        CRIME_MONTH_COL_NAME: 'CrimeMonth'
    })
    
    # 輸出除錯 CSV (包含 geometry 欄位狀態)
    DEBUG_CSV_FILE = os.path.join(OUTPUT_DIR, 'auckland_crime_debug.csv')
    df_final.drop(columns=['geometry']).to_csv(DEBUG_CSV_FILE, index=False, encoding='utf-8') 
    print(f"✅ 除錯文件 (auckland_crime_debug.csv) 輸出到 {DEBUG_CSV_FILE}")

    # 檢查和刪除無效行
    missing_geometry_count = df_final['geometry'].isna().sum()
    print(f"   -> 🚨 檢查: 經過兩階段匹配後，缺少幾何圖形的記錄數: {missing_geometry_count}")
    
    # 刪除沒有有效幾何圖形、犯罪月份或犯罪類型的行
    df_final.dropna(subset=['geometry', 'CrimeMonth', 'OffenceType'], inplace=True)

    print(f"✅ 警察數據處理完成。最終用於分析的記錄數: {len(df_final)}。")
    if len(df_final) == 0 and len(df_auckland) > 0:
        print("⚠️ 警告: 所有奧克蘭記錄均由於缺乏幾何或必要信息而被刪除。")
    
    gdf_crime = gpd.GeoDataFrame(
        df_final.drop(columns=['MB_number', 'Territorial Authority']),
        geometry='geometry', 
        crs="EPSG:4326"
    )
        
    return gdf_crime[['OffenceType', 'PoliceDistrict', 'CrimeMonth', 'geometry']]


# --- 3. 獲取路線幾何 (保持不變) ---
def fetch_route_geometry() -> gpd.GeoDataFrame:
    """獲取巴士路線幾何數據。"""
    print("--- 2. 正在獲取 AT 路線幾何 ---")
    try:
        arcgis_response = requests.get(ARCGIS_ROUTES_URL)
        arcgis_response.raise_for_status() 
        gdf_routes = gpd.read_file(io.BytesIO(arcgis_response.content))
        
        gdf_routes.rename(columns={'ROUTENUMBER': 'Route No'}, inplace=True) 
        gdf_routes = gdf_routes[gdf_routes['MODE'] == 'Bus'].copy()
        gdf_routes = gdf_routes[['Route No', 'geometry']].copy()
        gdf_routes['Route No'] = gdf_routes['Route No'].astype(str)
        
        print(f"✅ 成功獲取 {len(gdf_routes)} 條巴士路線幾何。")
        return gdf_routes
    except Exception as e:
        print(f"❌ 獲取 ArcGIS 數據失敗: {e}")
        raise


# --- 4. 空間分析和數據彙總 (保持不變) ---

def analyze_and_aggregate(gdf_routes: gpd.GeoDataFrame, gdf_crime: gpd.GeoDataFrame):
    """執行空間連接、計算統計數據並生成 GeoJSON 和 JSON 文件。"""
    print("--- 3. 執行空間分析和數據彙總 ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True) 
    
    if gdf_crime.empty:
        print("⚠️ 警告：由於沒有有效的奧克蘭犯罪數據，跳過空間分析。")
        min_date = 'N/A'
        max_date = 'N/A'
        empty_geojson_output(gdf_routes) 
        empty_stats_output(min_date, max_date)
        return

    # 1. 創建 50 米緩衝區
    gdf_routes_proj = gdf_routes.to_crs(epsg=2193) 
    gdf_routes_buffer = gdf_routes_proj.copy()
    gdf_routes_buffer['geometry'] = gdf_routes_buffer.geometry.buffer(50) 
    
    # 2. 投影犯罪數據
    gdf_crime_proj = gdf_crime.to_crs(epsg=2193)
    
    # 3. 空間連接 (Spatial Join)
    crime_counts = gpd.sjoin(gdf_crime_proj, gdf_routes_buffer.reset_index(), how='inner', predicate='intersects')
    
    print(f"   -> 空間連接後的犯罪事件記錄數: {len(crime_counts)}") 

    if crime_counts.empty:
        print("⚠️ 警告：沒有犯罪事件落在任何巴士路線的 50 米緩衝區內。")
        min_date = 'N/A'
        max_date = 'N/A'
    else:
        min_date = crime_counts['CrimeMonth'].min().strftime('%Y-%m-%d')
        max_date = crime_counts['CrimeMonth'].max().strftime('%Y-%m-%d')

    # 5. 統計每條路線的犯罪總數
    total_crime_summary = crime_counts.groupby('index_right').size().reset_index(name='Total_Crime_Count')
    
    # 6. 彙總犯罪細節 (趨勢和類型)
    crime_details = {
        'metadata': {
            'crime_period_start': min_date,
            'crime_period_end': max_date,
            'buffer_distance_m': 50,
            'data_source': 'NZ Police (Full Available Dataset) merged with NZ Meshblock/Area Unit Geometry'
        },
        'routes': {}
    }
    
    for route_index in total_crime_summary['index_right'].unique():
        route_data = crime_counts[crime_counts['index_right'] == route_index]
        route_no = gdf_routes_buffer.loc[route_index, 'Route No']
        
        monthly_trend = route_data.groupby(route_data['CrimeMonth'].dt.to_period('M')).size().to_dict()
        monthly_trend = {str(k): int(v) for k, v in monthly_trend.items()}
        
        type_breakdown = route_data['OffenceType'].value_counts().to_dict()
        type_breakdown = {k: int(v) for k, v in type_breakdown.items()}
        
        crime_details['routes'][route_no] = {
            'monthly_trend': monthly_trend,
            'type_breakdown': type_breakdown
        }

    # 7. 將總犯罪計數合併回路線 GeoDataFrame
    gdf_results = gdf_routes_buffer.reset_index().merge(total_crime_summary, 
                                                        left_on='index', 
                                                        right_on='index_right', 
                                                        how='left')
    gdf_results['Total_Crime_Count'] = gdf_results['Total_Crime_Count'].fillna(0).astype(int)
    gdf_output = gdf_results.to_crs(epsg=4326)[['Route No', 'Total_Crime_Count', 'geometry']].copy()

    # 8. 儲存結果
    gdf_output.to_file(OUTPUT_FILE, driver='GeoJSON', encoding='utf-8')
    print(f"✅ GeoJSON 輸出到 {OUTPUT_FILE}")
    
    with open(STATS_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(crime_details, f, ensure_ascii=False, indent=4)
    print(f"✅ 犯罪細分統計輸出到 {STATS_OUTPUT_FILE}")

def empty_geojson_output(gdf_routes):
    # 創建一個空的 GeoJSON 輸出
    gdf_routes['Total_Crime_Count'] = 0
    gdf_routes = gdf_routes.to_crs(epsg=4326)[['Route No', 'Total_Crime_Count', 'geometry']].copy()
    gdf_routes.to_file(OUTPUT_FILE, driver='GeoJSON', encoding='utf-8')

def empty_stats_output(min_date, max_date):
    # 創建一個空的 JSON 輸出
    crime_details = {
        'metadata': {
            'crime_period_start': min_date,
            'crime_period_end': max_date,
            'buffer_distance_m': 50,
            'data_source': 'NZ Police (Full Available Dataset) merged with NZ Meshblock/Area Unit Geometry'
        },
        'routes': {}
    }
    with open(STATS_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(crime_details, f, ensure_ascii=False, indent=4)


# --- 5. 主流程 (Main Flow) ---
def run_etl():
    """運行 ETL 流程。"""
    if not POLICE_DATA_URL:
        print("❌ 錯誤：缺少 POLICE_DATA_URL 環境變量。請在 GitHub Secrets 中設置。")
        sys.exit(1)
        
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 傳遞 Area Unit URL
        gdf_crime = fetch_and_clean_police_data(POLICE_DATA_URL, MESHBLOCK_BASE_URL, AREA_UNIT_BASE_URL) 
        gdf_routes = fetch_route_geometry()
        analyze_and_aggregate(gdf_routes, gdf_crime)
        print("\n🎉 ETL 流程全部成功完成！")
    except Exception as e:
        error_message = str(e).strip()
        print(f"\n❌ ETL 流程中斷: {error_message}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
