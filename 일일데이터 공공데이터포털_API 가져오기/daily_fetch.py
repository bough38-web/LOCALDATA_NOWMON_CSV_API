import requests
import pandas as pd
import time
import urllib.parse
import logging
import json
import zipfile
import math
import argparse
import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# [NEW] Path setup to import from project root's src/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from src.notifier import EmailNotifier
except ImportError:
    # Fallback if structure varies
    EmailNotifier = None

# ==========================================
# 0. 로깅 설정
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api_extraction.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 2026년 대한민국 법정공휴일 (대체공휴일 포함)
# ==========================================
KOREAN_HOLIDAYS_2026 = {
    "2026-01-01", # 신정
    "2026-02-16", "2026-02-17", "2026-02-18", # 설날 연휴
    "2026-03-01", "2026-03-02", # 삼일절 (대체공휴일 포함)
    "2026-05-05", # 어린이날
    "2026-05-24", "2026-05-25", # 부처님오신날 (대체공휴일 포함)
    "2026-06-06", # 현충일
    "2026-08-15", "2026-08-17", # 광복절 (대체공휴일 포함)
    "2026-09-24", "2026-09-25", "2026-09-26", # 추석 연휴
    "2026-10-03", # 개천절
    "2026-10-09", # 한글날
    "2026-12-25"  # 성탄절
}

def is_korean_workday(dt):
    """주말(토, 일) 및 공휴일 여부를 판별합니다."""
    # dt.weekday(): 0(월) ~ 6(일)
    if dt.weekday() >= 5:
        return False
    if dt.strftime("%Y-%m-%d") in KOREAN_HOLIDAYS_2026:
        return False
    return True

# ==========================================
# [NEW] 체크포인트 매니저 클래스
# ==========================================
class CheckpointManager:
    def __init__(self, checkpoint_path):
        self.path = Path(checkpoint_path)
        self.data = self._load()

    def _load(self):
        if self.path.exists():
            try:
                with open(self.path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def is_completed(self, date_str, service_id, local_code):
        key = f"{date_str}:{service_id}:{local_code}"
        return self.data.get(key) == "COMPLETED"

    def mark_completed(self, date_str, service_id, local_code):
        key = f"{date_str}:{service_id}:{local_code}"
        self.data[key] = "COMPLETED"
        self._save()

    def _save(self):
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def clear(self):
        if self.path.exists():
            try:
                self.path.unlink()
            except: pass
        self.data = {}

def main():
    try:
        # ==========================================
        # 1. 아규먼트 파싱 및 모드 설정
        # ==========================================
        parser = argparse.ArgumentParser(description='Public Data Extraction Script (3-Day Rolling Sync)')
        parser.add_argument('--mode', type=str, default='DAILY', choices=['FULL', 'DAILY'], help='Extraction mode')
        parser.add_argument('--date', type=str, default='', help='Base target date (YYYY-MM-DD)')
        parser.add_argument('--days', type=int, default=1, help='Number of previous days to collect')
        parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers')
        parser.add_argument('--force', action='store_true', help='Force collection regardless of workday check')
        args = parser.parse_args()

        MODE = args.mode 
        MAX_WORKERS = args.workers
        DAYS_TO_FETCH = args.days

        # 기준 날짜 설정 (기본값: 어제)
        if args.date:
            base_date = datetime.strptime(args.date, "%Y-%m-%d")
        else:
            # 기본값: 오늘 기준 2일 전 (사용자 요청 반영: 마이너스 2일)
            base_date = datetime.now() - timedelta(days=2)

        # 수집할 날짜 리스트 생성 (평일/공휴일 제외 로직 적용, --force 시 무시)
        all_potential_dates = [base_date - timedelta(days=i) for i in range(DAYS_TO_FETCH)]
        target_dates = []
        for dt in all_potential_dates:
            dt_str = dt.strftime("%Y-%m-%d")
            if args.force or is_korean_workday(dt):
                target_dates.append(dt_str)
            else:
                logger.info(f"📅 [{dt_str}] 주말/공휴일 수집 제외 (강제 수집 시 --force 사용)")
        
        if not target_dates:
            logger.info(f"⏭️ 수집 대상 기간({DAYS_TO_FETCH}일분) 중 수집 대상 날짜가 없어 종료합니다.")
            return

        logger.info(f"📅 수집 대상 날짜 ({len(target_dates)}일분, Force={args.force}): {target_dates}")

        retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("http://", adapter); session.mount("https://", adapter)

        sheet_name = urllib.parse.quote("조회")
        SHEET_URL = f"https://docs.google.com/spreadsheets/d/1Y6n4OgetzmvJZBcq75oZRiriMWFSIh3L/gviz/tq?tqx=out:csv&sheet={sheet_name}"

        BASE_PATH = Path(__file__).resolve().parent
        PROJECT_ROOT = BASE_PATH.parent
        
        # Path logic: Look in PROJECT_ROOT for global assets
        API_KEY_PATH = PROJECT_ROOT / '오픈API' / 'api_key.txt'
        DATA_OUTPUT_PATH = PROJECT_ROOT / 'data'
        CONFIG_PATH = PROJECT_ROOT / 'src' / 'branch_config.json'
        # Checkpoint is local to the engine 
        CHECKPOINT_PATH = BASE_PATH / 'checkpoint.json'
        
        # Fallback for API_KEY if structure is different
        if not API_KEY_PATH.exists():
            API_KEY_PATH = BASE_PATH / '오픈API' / 'api_key.txt'

        DATA_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

        cp_manager = CheckpointManager(CHECKPOINT_PATH)

        # Load Targeted Sigungu Codes from Config
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                branch_config = json.load(f)
            TARGET_CODES = [code for b in branch_config['branches'] for code in b['codes']]
            logger.info(f"📍 대상 시군구 코드 로드 완료: {len(TARGET_CODES)}개")
        except:
            TARGET_CODES = ["3120000", "3000000", "3010000", "3020000", "3080000", "3940000", "3820000", "3990000", "4201000", "4191000"]
            logger.warning("📍 기본 시군구 코드를 사용합니다.")

        # ------------------------------------------
        # 기초 자료 로드 (공통)
        # ------------------------------------------
        df_urls = pd.read_csv(SHEET_URL, encoding='utf-8')
        
        # 구글 시트에서 매핑 자료 라이브 로드
        mapping_sheet_url = f"https://docs.google.com/spreadsheets/d/1fmWEc08sj48rif8hsFlgx8rfaCD4I-9z/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote('항목매핑')}"
        try:
            df_mapping = pd.read_csv(mapping_sheet_url, skiprows=2)
            mapping_dict = dict(zip(df_mapping.iloc[:, 4].dropna(), df_mapping.iloc[:, 5].dropna()))
        except Exception as e:
            logger.error(f"구글 시트 매핑 로드 실패: {e}")
            mapping_dict = {}

        def fetch_portal_data_page_raw(api_url, auth_key, page_no=1, local_code=None):
            decoded_key = urllib.parse.unquote(str(auth_key).strip())
            params = {'serviceKey': decoded_key, 'pageNo': page_no, 'numOfRows': 500, 'type': 'json'}
            if local_code:
                params['localCode'] = local_code
            try:
                resp = session.get(api_url, params=params, timeout=(20, 180))
                return resp.json() if resp.status_code == 200 else None
            except: return None

        def process_page(api_url, auth_key, page, target_date_str, local_code=None):
            res_json = fetch_portal_data_page_raw(api_url, auth_key, page, local_code)
            if not res_json: return [], "", ""
            items = res_json.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            if not items: return [], "", ""
            if not isinstance(items, list): items = [items]
            filtered = []
            max_d = ""; min_d = "9999-99-99"
            for item in items:
                addr = str(item.get('ROAD_NM_ADDR', '') or item.get('LOTNO_ADDR', '')).strip()
                updt = str(item.get('DAT_UPDT_PNT', ''))
                if updt:
                    if updt > max_d: max_d = updt
                    if updt < min_d: min_d = updt
                if target_date_str in updt:
                    # Region filtering done at API level (localCode)
                    filtered.append({mapping_dict.get(k, k): v for k, v in item.items()})
            return filtered, max_d, min_d

        def process_service_extraction(api_url, auth_key, target_date_str, local_code=None):
            first = fetch_portal_data_page_raw(api_url, auth_key, 1, local_code)
            if not first: return pd.DataFrame()
            total = first.get('response', {}).get('body', {}).get('totalCount', 0)
            if total == 0: return pd.DataFrame()
            pages = math.ceil(total / 500); collected = []
            batch = MAX_WORKERS * 2
            for b_start in range(1, pages + 1, batch):
                b_end = min(b_start + batch, pages + 1)
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {executor.submit(process_page, api_url, auth_key, p, target_date_str, local_code): p for p in range(b_start, b_end)}
                    for f in as_completed(futures):
                        rows, _, _ = f.result()
                        if rows: collected.extend(rows)
            return pd.DataFrame(collected) if collected else pd.DataFrame()

        # ------------------------------------------
        # 메인 루프 (날짜별 순회)
        # ------------------------------------------
        all_collected_files = []
        total_records_all = 0
        summary_details = ""

        # 임시 작업 디렉토리
        TEMP_ROOT = BASE_PATH / "TEMP_BATCH_WORK"
        if TEMP_ROOT.exists(): shutil.rmtree(TEMP_ROOT)
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)

        # [NEW] Periodic progress email tracking
        last_progress_email_time = time.time()
        start_time_all = datetime.now()

        for t_date in target_dates:
            logger.info(f"🚀 [{t_date}] 데이터 수집 시작")
            date_dir = TEMP_ROOT / t_date.replace("-", "")
            date_dir.mkdir(parents=True, exist_ok=True)
            daily_count = 0
            
            for idx, row in df_urls.iterrows():
                svc_full = str(row.iloc[1]); oper = str(row.iloc[2]); api_url = str(row.iloc[3])
                
                # [FIX] Sanitize svc_id and oper for safe filenames
                raw_svc_id = str(row.iloc[7]) if not pd.isna(row.iloc[7]) else f"ID_{idx+1}"
                svc_id = "".join([c if c.isalnum() or c in ('-', '_') else '_' for c in raw_svc_id])
                safe_oper = "".join([c if c.isalnum() or c in ('-', '_') else '_' for c in oper])
                
                S_KEY = os.environ.get('SERVICE_KEY')
                if not S_KEY:
                    try:
                        with open(API_KEY_PATH, 'r', encoding='utf-8') as f: S_KEY = f.read().strip()
                    except: S_KEY = "DvyS97s/WyCWPJjBU7bvoebRE+4lxRphMHewhAcQQrGMPT/8PcP0bOCO8bTs2b7H25qViKWruSqim57HphOAjA=="
                if "apis.data.go.kr" not in api_url or not S_KEY: continue
                
                for l_code in TARGET_CODES:
                    # [NEW] 체크포인트 확인
                    if cp_manager.is_completed(t_date, svc_id, l_code):
                        logger.info(f"⏭️ Skipping (Completed): {t_date} {svc_id} {l_code}")
                        continue

                    df_daily = process_service_extraction(api_url, S_KEY, t_date, l_code)
                    if not df_daily.empty:
                        fname = f"{t_date.replace('-','')}_{l_code}_{svc_id[:20]}_{safe_oper[:20]}.csv"
                        out_path = date_dir / fname
                        df_daily.to_csv(out_path, index=False, encoding='cp949')
                        all_collected_files.append(out_path)
                        cnt = len(df_daily); daily_count += cnt
                        total_records_all += cnt
                    
                    # [NEW] 성공 시 체크포인트 기록
                    cp_manager.mark_completed(t_date, svc_id, l_code)

                    # [NEW] Periodic progress report (every 30 minutes)
                    if EmailNotifier and (time.time() - last_progress_email_time >= 1800):
                        try:
                            elapsed = datetime.now() - start_time_all
                            progress_msg = f"""[진행 현황 보고]
- 수집 시작: {start_time_all.strftime('%H:%M:%S')}
- 경과 시간: {str(elapsed).split('.')[0]}
- 현재까지 발견 건수: {total_records_all}건
- 현재 작업 중인 날짜: {t_date}
- 현재 작업 중인 서비스: {svc_id} ({safe_oper})
- 현재까지 생성된 파일: {len(all_collected_files)}개
"""
                            notifier = EmailNotifier()
                            notifier.send_progress_report(progress_msg)
                            last_progress_email_time = time.time()
                            logger.info("📡 30분 주기 중간 보고 메일 발송 완료")
                        except Exception as e:
                            logger.warning(f"⚠️ 중간 보고 메일 발송 실패: {e}")

            summary_details += f"- {t_date}: {daily_count}건 발견\n"
            logger.info(f"🏁 [{t_date}] 수집 종료 (총 {daily_count}건)")

        # ------------------------------------------
        # 압축 및 리포트 생성
        # ------------------------------------------
        zip_path = DATA_OUTPUT_PATH / "LOCALDATA_YESTERDAY_CSV.zip"
        if all_collected_files:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for f in all_collected_files: zf.write(f, f.name)
            
            summary_content = f"""[영업기회 데이터 취합 자동 리포트]
기준 범위: {target_dates[-1]} ~ {target_dates[0]} ({DAYS_TO_FETCH}일간)
작성 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (KST)

■ 수집 요약
- 전체 신규 건수: {total_records_all}건
- 생성된 파일 수: {len(all_collected_files)}개

■ 날짜별 통계
{summary_details}
※ 최근 3일간의 변동분을 매일 자동으로 중복 체크하여 보정하고 있습니다.
※ 상세 데이터는 깃허브 저장소(data/)와 웹 앱에서 확인하실 수 있습니다.
"""
            with open(BASE_PATH.parent / "summary.txt", "w", encoding="utf-8") as fs: fs.write(summary_content)
            logger.info(f"✨ 전체 압축 및 요약 완료: {zip_path.name}")
        else:
            msg = f"[{target_dates[-1]}~{target_dates[0]}] 기간 동안 신규 변동 데이터가 없습니다."
            with open(BASE_PATH.parent / "summary.txt", "w", encoding="utf-8") as fs: fs.write(msg)
            logger.warning(f"⚠️ {msg}")

        if TEMP_ROOT.exists(): shutil.rmtree(TEMP_ROOT)
        
        # [NEW] Send Email Notification on Completion
        if EmailNotifier:
            try:
                notifier = EmailNotifier()
                notifier.send_sync_report(summary_content if 'summary_content' in locals() else (msg if 'msg' in locals() else "수집이 완료되었습니다."))
            except Exception as mail_e:
                logger.warning(f"⚠️ 이메일 알림 발송 중 오류발생 (프로세스는 정상종료): {mail_e}")

        # [NEW] 수집이 모두 정상적으로 끝났다면 체크포인트 삭제
        cp_manager.clear()
        
        logger.info("Done.")

    except Exception as e:
        logger.error(f"💥 Error: {e}", exc_info=True)
        with open(BASE_PATH.parent / "summary.txt", "w", encoding="utf-8") as fs:
            fs.write(f"자동화 실행 중 오류가 발생했습니다:\n{e}")
        exit(1)

if __name__ == "__main__":
    main()
