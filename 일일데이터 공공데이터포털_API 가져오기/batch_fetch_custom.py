import subprocess
import os
import pandas as pd
import zipfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta

# ==========================================
# 0. 설정
# ==========================================
START_DATE = datetime(2026, 3, 28)
END_DATE = datetime(2026, 4, 2)

BASE_PATH = Path(__file__).resolve().parent
DATA_PATH = BASE_PATH.parent / 'data'
DATA_PATH.mkdir(parents=True, exist_ok=True)
BATCH_TEMP = BASE_PATH / 'CUSTOM_BATCH_TEMP'
BATCH_TEMP.mkdir(parents=True, exist_ok=True)

collected_files = []
summary_data = []

current_date = START_DATE
while current_date <= END_DATE:
    date_str = current_date.strftime("%Y-%m-%d")
    print(f"🚀 Processing {date_str}...")
    
    # Run daily_fetch.py
    result = subprocess.run(
        ["python3", "daily_fetch.py", "--mode", "DAILY", "--date", date_str],
        cwd=BASE_PATH,
        capture_output=True,
        text=True
    )
    
    zip_path = DATA_PATH / "LOCALDATA_YESTERDAY_CSV.zip"
    
    if zip_path.exists():
        print(f"   ✅ Data found for {date_str}. Extracting...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for member in zf.namelist():
                zf.extract(member, BATCH_TEMP)
                extracted_path = BATCH_TEMP / member
                try:
                    df = pd.read_csv(extracted_path, encoding='cp949')
                    count = len(df)
                    # 유형 추출 from filename
                    parts = member.split('_')
                    if len(parts) >= 4:
                        oper_name = "_".join(parts[3:-1]).replace("_데이터_조회", "").replace("_조회", "")
                    else:
                        oper_name = "Unknown"
                    summary_data.append({'일자': date_str, '유형': oper_name, '건수': count})
                except Exception as e:
                    print(f"      ⚠️ Error reading {member}: {e}")
        
        os.remove(zip_path)
    else:
        print(f"   ℹ️ No data for {date_str}.")
        
    current_date += timedelta(days=1)

# ==========================================
# 1. 요약 리포트 생성
# ==========================================
if summary_data:
    df_summary = pd.DataFrame(summary_data)
    pivot_summary = df_summary.pivot_table(index='유형', columns='일자', values='건수', aggfunc='sum', fill_value=0)
    pivot_summary.loc['Total'] = pivot_summary.sum()
    pivot_summary['Total'] = pivot_summary.sum(axis=1)
    
    summary_path = BATCH_TEMP / "CUSTOM_SUMMARY.csv"
    pivot_summary.to_csv(summary_path, encoding='cp949')
    
    final_zip = DATA_PATH / "LOCALDATA_CUSTOM_TOTAL.zip"
    print(f"📦 Creating final ZIP: {final_zip.name}...")
    with zipfile.ZipFile(final_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
        for f in BATCH_TEMP.glob("*.csv"):
            zf.write(f, f.name)
    
    print(f"✨ Success! Saved to {final_zip.absolute()}")
    print(pivot_summary)
    # shutil.rmtree(BATCH_TEMP) # don't remove temp immediately so we can inspect
else:
    print("⚠️ No data collected for the entire period.")
