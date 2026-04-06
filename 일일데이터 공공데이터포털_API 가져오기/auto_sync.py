import os
import sys
import logging
import subprocess
from datetime import datetime
from pathlib import Path

# Fix path to import from src
BASE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_PATH.parent
sys.path.append(str(PROJECT_ROOT))

# ==========================================
# 자동화 설정 (Standalone Module 버전)
# ==========================================
# 스크립트 위치 기준 경로 설정
BASE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_PATH.parent
EXTRACTION_SCRIPT = BASE_PATH / "daily_fetch.py"
LOG_FILE = BASE_PATH / "auto_sync.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def run_command(command, cwd=None):
    """쉘 명령어 실행 및 결과 로깅"""
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True,
            cwd=cwd
        )
        stdout, stderr = process.communicate()
        if process.returncode == 0:
            logger.info(f"성공: {command.split()[0]}... 완료")
            return True, stdout
        else:
            logger.error(f"실패: {command}\nError: {stderr}")
            return False, stderr
    except Exception as e:
        logger.error(f"예외 발생: {e}")
        return False, str(e)

def main():
    logger.info("==========================================")
    logger.info(f"매일 자동 동기화 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 데이터 추출 실행 (DAILY 모드 - 어제 데이터 수집)
    logger.info("1단계: 데이터 추출 엔진 가동 (daily_fetch.py)...")
    success, output = run_command(f'python3 "{EXTRACTION_SCRIPT}" --mode DAILY', cwd=PROJECT_ROOT)
    
    if not success:
        logger.error("데이터 추출 중 오류가 발생하여 중단합니다.")
        return

    # 2. 깃허브 반영 (Commit & Push)
    logger.info("2단계: 깃허브 자동 커밋 및 푸시...")
    
    # 변경사항 스테이징 (data 폴더 내 ZIP 파일 포함)
    run_command("git add .", cwd=PROJECT_ROOT)
    
    # 커밋 메시지 생성
    commit_msg = f"Auto-Update: Daily License Data ({datetime.now().strftime('%Y-%m-%d')})"
    # 커밋 시도 (변경사항이 없으면 실패할 수 있음)
    success, _ = run_command(f'git commit -m "{commit_msg}"', cwd=PROJECT_ROOT)
    
    if success:
        logger.info("3단계: 로컬 깃허브 원격 저장소로 푸시 중... (GitHub Actions에서는 자동 처리되므로 로컬 전용입니다)")
        push_success, _ = run_command("git push origin main", cwd=PROJECT_ROOT)
        if push_success:
            logger.info("✨ 모든 로컬 동기화 프로세스가 성공적으로 완료되었습니다.")
        else:
            logger.error("푸시 실패. 네트워크 상태나 로컬 깃 권한을 확인하세요.")
    else:
        logger.info("ℹ️ 변경된 데이터가 없어 커밋을 스킵합니다.")

    # 4. 리포트 이메일 전송 (New)
    logger.info("4단계: 결과 리포트 이메일 전송 시도...")
    try:
        from src.notifier import EmailNotifier
        notifier = EmailNotifier()
        
        summary_path = PROJECT_ROOT / "summary.txt"
        if summary_path.exists():
            with open(summary_path, "r", encoding="utf-8") as f:
                summary_content = f.read()
            
            # 리포트 갱신 스크립트 실행 (최신 신규/폐업 통계 포함)
            try:
                subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "generate_daily_report.py")], capture_output=True)
            except: pass

            notifier.send_sync_report(summary_content)
        else:
            logger.warning("summary.txt 파일이 없어 이메일을 전송하지 못했습니다.")
    except Exception as e:
        logger.error(f"이메일 전송 중 오류 발생: {e}")

if __name__ == "__main__":
    main()
