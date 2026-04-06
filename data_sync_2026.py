
import zipfile
import pandas as pd
import io
import os
import subprocess
import sys

def run_sync():
    print("🚀 Starting 2026 Data Sync Automation...")
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    zip_path = os.path.join(base_dir, 'data', 'LOCALDATA_NOWMON_CSV_3월_최종.zip')
    output_zip = os.path.join(base_dir, 'data', 'LOCALDATA_2026_ONLY.zip')
    output_csv = os.path.join(base_dir, 'data', 'LOCALDATA_2026_ONLY.csv')

    if not os.path.exists(zip_path):
        print(f"❌ Error: {zip_path} not found.")
        return

    # 1. Extraction & Filtering
    print("📦 Extracting and filtering data for 2026+...")
    all_data = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv') and '__MACOSX' not in f]
            for f in csv_files:
                with z.open(f) as file:
                    df = pd.read_csv(file, encoding='cp949', on_bad_lines='skip', dtype=str)
                    if '인허가일자' in df.columns:
                        # Filter for 2026 onwards
                        df_2026 = df[df['인허가일자'] >= '2026']
                        if not df_2026.empty:
                            all_data.append(df_2026)
        
        if not all_data:
            print("⚠️ No 2026+ data found in the ZIP.")
            return

        final_df = pd.concat(all_data, ignore_index=True)
        
        # [NEW] Deduplication within sync script
        print("🧹 Deduplicating data...")
        # 1. Sort by date
        if '인허가일자' in final_df.columns:
            final_df['인허가일자'] = pd.to_datetime(final_df['인허가일자'], errors='coerce')
            
            # [NEW] Exclude Weekends (Sat=5, Sun=6)
            print("🗓 Filtering out weekends...")
            final_df = final_df[final_df['인허가일자'].dt.weekday < 5]
            
            # [NEW] Log unique dates for verification
            unique_dates = final_df['인허가일자'].dt.strftime('%Y-%m-%d').dropna().unique()
            print(f"📊 Unique dates in data: {sorted(list(unique_dates))}")
            
            final_df.sort_values(by='인허가일자', ascending=False, inplace=True, na_position='last')
        
        # 2. Drop duplicates (name + addr)
        def simple_key(row):
            t = str(row.get('사업장명', '')).strip()
            a = str(row.get('소재지전체주소', '') or row.get('도로명전체주소', '') or '').strip()
            return f"{t}_{a}"
        
        final_df['_sync_key'] = final_df.apply(simple_key, axis=1)
        final_df.drop_duplicates(subset=['_sync_key'], keep='first', inplace=True)
        final_df.drop(columns=['_sync_key'], inplace=True)
        
        print(f"✅ Final row count after deduplication & weekend filter: {len(final_df)}")
        print("💾 Saving optimized output...")
        
        # Convert back to string for CSV saving (optional, but good for consistency)
        final_df['인허가일자'] = final_df['인허가일자'].dt.strftime('%Y%m%d')
        final_df.to_csv(output_csv, index=False, encoding='cp949')
        
        with zipfile.ZipFile(output_zip, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            z.write(output_csv, arcname='LOCALDATA_2026_ONLY.csv')
        
        # Keep the CSV for local use if needed, or remove it to save space
        # os.remove(output_csv) 
        print(f"✅ Created {output_zip}")

        # 3. Git Deployment
        print("🌍 Pushing to GitHub for web deployment...")
        try:
            # Check for secrets/large files safety
            subprocess.run(["rm", "-rf", os.path.join(base_dir, "key")], check=False)
            
            subprocess.run(["git", "add", "data/LOCALDATA_2026_ONLY.zip"], check=True)
            subprocess.run(["git", "commit", "-m", "data: [AUTO] Update 2026 data optimized zip"], check=False)
            
            result = subprocess.run(["git", "push", "origin", "main"], capture_output=True, text=True)
            if result.returncode == 0:
                print("🏁 Success! Web app will be updated in about 1 minute.")
            else:
                print(f"⚠️ Git Push Failed:\n{result.stderr}")
                
        except Exception as git_err:
            print(f"❌ Git Command Error: {git_err}")

    except Exception as e:
        print(f"❌ Automation Failed: {e}")

if __name__ == "__main__":
    run_sync()
