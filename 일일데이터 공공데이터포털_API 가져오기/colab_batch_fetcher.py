# @title **Public Data Portal Batch Fetcher (Google Drive Sync)**
# @markdown ---
# @markdown ### **1. 작업 기간 설정**
# @markdown 3월 1일부터 수집할 기간을 입력하세요.
START_DATE_STR = "2026-03-01" # @param {type:"string"}
END_DATE_STR = "2026-03-21" # @param {type:"string"}
# @markdown ### **2. 구글 드라이브 업로드 설정**
# @markdown 결과 파일이 저장될 구글 드라이브 폴더 ID입니다.
GDRIVE_FOLDER_ID = "1p5DeA-JEfKEKZdL0TnSIHIAH8p4U1JPE" # @param {type:"string"}
# @markdown ---

import requests
import pandas as pd
import time
import urllib.parse
import zipfile
import math
import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# 코랩 및 구글 인증 관련 라이브러리
from google.colab import files, auth, drive
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def setup_environment():
    print("📦 필수 라이브러리 및 환경 구축 중...")
    !pip install -q openpyxl requests pandas google-api-python-client
    
    # 구글 계정 인증 (드라이브 API 접근을 위함)
    print("🔑 구글 계정 인증을 진행합니다...")
    auth.authenticate_user()
    
    # 드라이브 마운트 (선택 사항이지만 파일 시스템 접근을 위해 권장)
    # drive.mount('/content/drive')
    print("✅ 라이브러리 및 인증 준비 완료")

# ==========================================
# 구글 드라이브 업로드 함수 (API 기반)
# ==========================================
def upload_to_gdrive(file_path, folder_id):
    """Google Drive API를 사용하여 특정 폴더에 파일을 업로드합니다."""
    try:
        service = build('drive', 'v3')
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='application/zip')
        file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        print(f"✅ 드라이브 업로드 성공! 파일 ID: {file.get('id')}")
        print(f"🔗 링크: {file.get('webViewLink')}")
        return file.get('webViewLink')
    except Exception as e:
        print(f"❌ 드라이브 업로드 중 오류 발생: {e}")
        return None

# ==========================================
# 핵심 수집 엔진 (최적화 버전)
# ==========================================
def fetch_portal_data_page_raw(session, api_url, auth_key, page_no=1):
    query = f"serviceKey={auth_key}&pageNo={page_no}&numOfRows=500&type=json"
    full_url = f"{api_url}?{query}"
    try:
        resp = session.get(full_url, timeout=(20, 180))
        if resp.status_code != 200: return None
        return resp.json()
    except: return None

def process_page_range(session, api_url, auth_key, page, date_range_prefix, target_regions, mapping_dict):
    res_json = fetch_portal_data_page_raw(session, api_url, auth_key, page)
    if not res_json: return []
    items_container = res_json.get('response', {}).get('body', {}).get('items', {})
    if not items_container: return []
    data_list = items_container.get('item', [])
    if not data_list: return []
    if not isinstance(data_list, list): data_list = [data_list]
    
    filtered_rows = []
    for item in data_list:
        addr = str(item.get('ROAD_NM_ADDR', '') or item.get('LOTNO_ADDR', '')).strip()
        updt_pnt = str(item.get('DAT_UPDT_PNT', ''))
        if any(reg in addr for reg in target_regions) and date_range_prefix in updt_pnt:
            mapped_item = {mapping_dict.get(k, k): v for k, v in item.items()}
            filtered_rows.append(mapped_item)
    return filtered_rows

def process_service_batch(session, api_url, auth_key, oper_name, date_range_prefix, target_regions, mapping_dict):
    first_res = fetch_portal_data_page_raw(session, api_url, auth_key, 1)
    if not first_res: return pd.DataFrame()
    body = first_res.get('response', {}).get('body', {})
    total_count = body.get('totalCount', 0)
    if total_count == 0: return pd.DataFrame()
    
    total_pages = math.ceil(total_count / 500)
    print(f"   ... [{oper_name}] 총 {total_count}건, {total_pages} 페이지 스캔 중...")
    
    all_collected_rows = []
    max_workers = 15
    batch_size = max_workers * 4
    for batch_start in range(1, total_pages + 1, batch_size):
        batch_end = min(batch_start + batch_size, total_pages + 1)
        pages_to_fetch = range(batch_start, batch_end)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {executor.submit(process_page_range, session, api_url, auth_key, p, date_range_prefix, target_regions, mapping_dict): p for p in pages_to_fetch}
            for future in as_completed(future_to_page):
                try:
                    rows = future.result()
                    if rows: all_collected_rows.extend(rows)
                except: pass
    return pd.DataFrame(all_collected_rows) if all_collected_rows else pd.DataFrame()

# ==========================================
# 실행 메인 프로세스
# ==========================================
def main():
    setup_environment()
    
    print("\n📁 매핑 엑셀 파일(LOCALDATA_...xlsx)을 업로드해 주세요.")
    uploaded = files.upload()
    if not uploaded:
        print("❌ 파일이 업로드되지 않았습니다.")
        return
    mapping_filename = list(uploaded.keys())[0]
    
    sheet_name = urllib.parse.quote("조회")
    SHEET_URL = f"https://docs.google.com/spreadsheets/d/1Y6n4OgetzmvJZBcq75oZRiriMWFSIh3L/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    
    try:
        df_urls = pd.read_csv(SHEET_URL, encoding='utf-8')
        df_mapping = pd.read_excel(mapping_filename, sheet_name='항목매핑', skiprows=2)
        mapping_dict = dict(zip(df_mapping.iloc[:, 4].dropna(), df_mapping.iloc[:, 5].dropna()))
        print("✅ 기초 자료 및 매핑 로드 완료")
    except Exception as e:
        print(f"❌ 초기 로딩 실패: {e}")
        return

    START_DATE = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
    END_DATE = datetime.strptime(END_DATE_STR, "%Y-%m-%d")
    DATE_PREFIX = START_DATE_STR[:7]
    TARGET_REGIONS = ["서울특별시", "경기도", "강원도", "강원특별자치도"]
    
    OUTPUT_DIR = Path("BATCH_RESULT")
    if OUTPUT_DIR.exists(): shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    session = requests.Session()
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=3))
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))

    collected_files = []
    summary_data = []

    print(f"\n🚀 배치 수집 시작: {START_DATE_STR} ~ {END_DATE_STR}")

    for idx, row in df_urls.iterrows():
        svc_full_name = str(row.iloc[1])
        oper_name = str(row.iloc[2])
        api_url = str(row.iloc[3])
        svc_id_raw = str(row.iloc[7]) if not pd.isna(row.iloc[7]) else f"ID_{idx+1}"
        auth_key = str(row.iloc[5])
        if auth_key == 'nan' or not auth_key:
            auth_key = "DvyS97s/WyCWPJjBU7bvoebRE+4lxRphMHewhAcQQrGMPT/8PcP0bOCO8bTs2b7H25qViKWruSqim57HphOAjA=="
        if "apis.data.go.kr" not in api_url: continue

        print(f"🔎 [{idx+1}] {svc_full_name} 스캔 중...")
        df_service = process_service_batch(session, api_url, auth_key, oper_name, DATE_PREFIX, TARGET_REGIONS, mapping_dict)
        
        if not df_service.empty:
            current_dt = START_DATE
            found_any = False
            while current_dt <= END_DATE:
                day_str = current_dt.strftime("%Y-%m-%d")
                df_day = df_service[df_service['최종수정시점'].str.contains(day_str, na=False)]
                if not df_day.empty:
                    found_any = True
                    count = len(df_day)
                    filename = f"{day_str.replace('-','')}_{svc_id_raw}_{oper_name.replace('/','_')}.csv"
                    df_day.to_csv(OUTPUT_DIR / filename, index=False, encoding='cp949')
                    collected_files.append(OUTPUT_DIR / filename)
                    summary_data.append({'일자': day_str, '유형': oper_name, '건수': count})
                current_dt += timedelta(days=1)
            if found_any: print(f"   ✅ 해당 기간 데이터 수집 완료")
    
    if summary_data:
        df_sum = pd.DataFrame(summary_data)
        pivot = df_sum.pivot_table(index='유형', columns='일자', values='건수', aggfunc='sum', fill_value=0)
        pivot.loc['Total'] = pivot.sum()
        pivot['Total'] = pivot.sum(axis=1)
        pivot.to_csv(OUTPUT_DIR / "BATCH_SUMMARY.csv", encoding='cp949')
        collected_files.append(OUTPUT_DIR / "BATCH_SUMMARY.csv")
        
        zip_name = f"BATCH_RESULT_{START_DATE_STR.replace('-','')}_{END_DATE_STR.replace('-','')}.zip"
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
            for f in collected_files:
                zf.write(f, f.name)
        
        print(f"\n✨ 작업 완료! 생성 파일: {zip_name}")
        print(f"📤 구글 드라이브 업로드 중...")
        upload_link = upload_to_gdrive(zip_name, GDRIVE_FOLDER_ID)
        
        if upload_link:
            print(f"\n📍 결과가 공유 폴더에 업로드되었습니다. 아래 링크에서 확인하세요:")
            print(upload_link)
        else:
            files.download(zip_name)
        
        shutil.rmtree(OUTPUT_DIR)
    else:
        print("\n⚠️ 선택한 기간 내에 추출된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
