import requests
import pandas as pd
import time
import urllib.parse
import logging
import json
import zipfile
import math
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# 0. 로깅 설정
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api_extraction_v4.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 아규먼트 파싱 및 모드 설정
# ==========================================
parser = argparse.ArgumentParser(description='Public Data Extraction Script')
parser.add_argument('--mode', type=str, default='DAILY', choices=['FULL', 'DAILY'], help='Extraction mode')
parser.add_argument('--date', type=str, default='', help='Target date (YYYY-MM-DD)')
parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers')
args = parser.parse_args()

MODE = args.mode 
MAX_WORKERS = args.workers

# 날짜 자동 계산 (오늘 19일 -> 16일 추출용)
if args.date:
    TARGET_DATE = args.date
else:
    TARGET_DATE = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("http://", adapter)
session.mount("https://", adapter)

sheet_name = urllib.parse.quote("조회")
SHEET_URL = f"https://docs.google.com/spreadsheets/d/1Y6n4OgetzmvJZBcq75oZRiriMWFSIh3L/gviz/tq?tqx=out:csv&sheet={sheet_name}"

BASE_PATH = Path(__file__).resolve().parent
ETC_PATH = BASE_PATH / '기타자료'
# GitHub Actions supports the main projekt root as CWD. 
# We want to output ZIP to the main project's 'data' folder.
DATA_OUTPUT_PATH = BASE_PATH.parent / 'data'
MAPPING_FILE = ETC_PATH / 'LOCALDATA_공공데이터포털 지방행정인허가 칼럼 매핑 자료_v3 (2).xlsx'

# 대상 지역 접두어
TARGET_REGIONS = ["서울특별시", "경기도", "강원도", "강원특별자치도", "인천광역시", "인천"]

# ==========================================
# 2. 기초 자료 로드
# ==========================================
try:
    df_urls = pd.read_csv(SHEET_URL, encoding='utf-8')
    df_mapping = pd.read_excel(MAPPING_FILE, sheet_name='항목매핑', skiprows=2)
    mapping_dict = dict(zip(df_mapping.iloc[:, 4].dropna(), df_mapping.iloc[:, 5].dropna()))
    logger.info(f"✅ 기초 자료 및 매핑 로드 완료 (Mode: {MODE}, Target Date: {TARGET_DATE}, Workers: {MAX_WORKERS})")
except Exception as e:
    logger.error(f"❌ 초기 로딩 실패: {e}")
    exit()

def fetch_portal_data_page_raw(api_url, auth_key, page_no=1):
    decoded_key = urllib.parse.unquote(str(auth_key).strip())
    params = {
        'serviceKey': decoded_key,
        'pageNo': page_no,
        'numOfRows': 500,
        'type': 'json'
    }
    try:
        resp = session.get(api_url, params=params, timeout=(20, 180))
        if resp.status_code != 200: return None
        return resp.json()
    except: return None

def process_page(api_url, auth_key, page, target_date_str):
    """단일 페이지 처리 및 데이터 추출"""
    res_json = fetch_portal_data_page_raw(api_url, auth_key, page)
    if not res_json: return []
    
    items_container = res_json.get('response', {}).get('body', {}).get('items', {})
    if not items_container: return []
    
    data_list = items_container.get('item', [])
    if not data_list: return []
    if not isinstance(data_list, list): data_list = [data_list]
    
    filtered_rows = []
    max_date_in_page = ""
    min_date_in_page = "9999-99-99"

    for item in data_list:
        addr = str(item.get('ROAD_NM_ADDR', '') or item.get('LOTNO_ADDR', '')).strip()
        updt_pnt = str(item.get('DAT_UPDT_PNT', ''))
        
        if updt_pnt:
            if updt_pnt > max_date_in_page: max_date_in_page = updt_pnt
            if updt_pnt < min_date_in_page: min_date_in_page = updt_pnt

        # 주소(서울/경기/강원) AND Target Date (D-3 등) 필터
        if any(reg in addr for reg in TARGET_REGIONS) and target_date_str in updt_pnt:
            mapped_item = {mapping_dict.get(k, k): v for k, v in item.items()}
            filtered_rows.append(mapped_item)
            
    return filtered_rows, max_date_in_page, min_date_in_page

def process_service_extraction(api_url, auth_key, service_id, target_date_str):
    """일일 변동분 추출 및 지역 필터링 (멀티스레드 최적화)"""
    first_res = fetch_portal_data_page_raw(api_url, auth_key, 1)
    if not first_res: return pd.DataFrame()
    
    body = first_res.get('response', {}).get('body', {})
    total_count = body.get('totalCount', 0)
    if total_count == 0: return pd.DataFrame()
    
    total_pages = math.ceil(total_count / 500)
    all_collected_rows = []
    
    logger.info(f"   ... 총 {total_count}건, {total_pages} 페이지 처리 시작 (Workers: {MAX_WORKERS})")

    # [OPTIMIZATION] Batch processed with ThreadPoolExecutor
    batch_size = MAX_WORKERS * 2
    stop_service = False

    for batch_start in range(1, total_pages + 1, batch_size):
        if stop_service: break
        
        batch_end = min(batch_start + batch_size, total_pages + 1)
        pages_to_fetch = range(batch_start, batch_end)
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {executor.submit(process_page, api_url, auth_key, p, target_date_str): p for p in pages_to_fetch}
            
            # Use sorted results to potentially trigger early stop
            results = {}
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    rows, p_max, p_min = future.result()
                    results[page_num] = (rows, p_max, p_min)
                except Exception as e:
                    logger.warning(f"      ⚠️ 페이지 {page_num} 처리 중 오류: {e}")

            for p in sorted(results.keys()):
                rows, p_max, p_min = results[p]
                if rows:
                    all_collected_rows.extend(rows)
                
                # [EARLY EXIT LOGIC] 
                # 만약 현재 페이지의 가장 최신 날짜(p_max)가 타겟 날짜보다 과거라면, 
                # 그리고 데이터가 보통 최신순 정렬이라면 (내림차순 가정), 이후 페이지는 볼 필요 없음.
                # 단, 지방행정인허가 API는 정렬이 일정치 않을 수 있으므로 'DAILY' 모드에서는 
                # 타겟 날짜보다 훨씬 과거(예: 타겟-10일)인 경우에만 조기 종료를 검토하거나 
                # 안전하게 전체 순회하되 병렬화로 속도만 개선함.
                # 여기서는 병렬화에 집중함.
        
        if batch_start % (MAX_WORKERS * 4) == 1:
            logger.info(f"      ... {batch_start}/{total_pages} 페이지 진행 중...")

    return pd.DataFrame(all_collected_rows) if all_collected_rows else pd.DataFrame()

    return pd.concat(service_data_list, ignore_index=True) if service_data_list else pd.DataFrame()

# ==========================================
# 3. 메인 수집 루프
# ==========================================
logger.info(f"🚀 일일 변동분 수집 시작 ({TARGET_DATE} 기준)")

DAILY_DIR = ETC_PATH / f'DAILY_CHANGE_{TARGET_DATE.replace("-","")}'
DAILY_DIR.mkdir(parents=True, exist_ok=True)

collected_files = []

for idx, row in df_urls.iterrows():
    svc_full_name = str(row.iloc[1])
    oper_name = str(row.iloc[2])
    api_url = str(row.iloc[3])
    # 시트 내에 별도 서비스코드(01_02_03_P 등)가 있는지 확인 (없으면 인덱스 등 사용)
    # 샘플 파일명: 20260317_10_34_01_P_...
    # 여기서는 시트 7번째 컬럼 등에 서비스코드가 있다고 가정하거나, URL에서 추출
    svc_id_raw = str(row.iloc[7]) if not pd.isna(row.iloc[7]) else f"ID_{idx+1}"
    
    auth_key = str(row.iloc[9]) if not pd.isna(row.iloc[9]) else str(row.iloc[5])
    
    # [FIX] Fallback key for missing entries in Google Sheet
    if auth_key == 'nan' or not auth_key:
        auth_key = "DvyS97s/WyCWPJjBU7bvoebRE+4lxRphMHewhAcQQrGMPT/8PcP0bOCO8bTs2b7H25qViKWruSqim57HphOAjA=="

    if "apis.data.go.kr" not in api_url or not auth_key: continue

    logger.info(f"🔎 [{idx+1}] {svc_full_name} 조회 중...")
    
    df_daily = process_service_extraction(api_url, auth_key, svc_id_raw, TARGET_DATE)
    
    if not df_daily.empty:
        # 파일명 형식: 20260317_서비스아이디_서비스명.csv
        file_date = TARGET_DATE.replace("-", "")
        safe_svc_name = oper_name.replace("/", "_").replace(" ", "_")
        filename = f"{file_date}_{svc_id_raw}_{safe_svc_name}.csv"
        output_path = DAILY_DIR / filename
        
        df_daily.to_csv(output_path, index=False, encoding='cp949') # 샘플과 동일하게 cp949 인코딩
        collected_files.append(output_path)
        logger.info(f"   ✅ {len(df_daily)}건 저장 완료")
    else:
        logger.info("   ℹ️ 변동 사항 없음")

    time.sleep(0.3)

# ==========================================
# 4. 압축 및 클린업
# ==========================================
if collected_files:
    zip_path = DATA_OUTPUT_PATH / f"LOCALDATA_DAILY_{TARGET_DATE.replace('-','')}.zip"
    logger.info(f"📦 결과물 압축 중... ({zip_path.name})")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for f in collected_files:
            zf.write(f, f.name)
    logger.info(f"✨ 최종 압축 완료: {zip_path.absolute()}")
else:
    logger.warning("⚠️ 오늘(타겟 날짜)의 변동 데이터가 없습니다.")

logger.info("🏁 작업 종료.")