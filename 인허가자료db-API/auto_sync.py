import subprocess
import logging
from datetime import datetime
import os

# ==========================================
# 자동화 설정
# ==========================================
# Get the root directory of the project (parent of this script's directory)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
EXTRACTION_SCRIPT = os.path.join(SCRIPT_DIR, "인허가자료추출_API.py")
LOG_FILE = os.path.join(SCRIPT_DIR, "auto_sync.log")

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
    
    # 1. 최신 코드 풀 (선택 사항)
    # run_command("git pull origin main", cwd=PROJECT_DIR)

    # 2. 데이터 추출 실행 (DAILY 모드)
    logger.info("1단계: 데이터 추출 엔진 가동...")
    success, output = run_command(f"python3 {EXTRACTION_SCRIPT} --mode DAILY", cwd=PROJECT_ROOT)
    
    if not success:
        logger.error("데이터 추출 중 오류가 발생하여 중단합니다.")
        return

    # 3. 깃허브 반영 (Commit & Push)
    logger.info("2단계: 깃허브 자동 커밋 및 푸시...")
    
    # 변경사항 스테이징 (기타자료 등 결과물 포함)
    run_command("git add .", cwd=PROJECT_ROOT)
    
    # 커밋 메시지 생성
    commit_msg = f"Auto-Update: Daily License Data ({datetime.now().strftime('%Y-%m-%d')})"
    success, _ = run_command(f'git commit -m "{commit_msg}"', cwd=PROJECT_ROOT)
    
    if success:
        logger.info("3단계: 원격 저장소로 푸시 중...")
        push_success, _ = run_command("git push origin main", cwd=PROJECT_ROOT)
        if push_success:
            logger.info("✨ 모든 자동화 프로세스가 성공적으로 완료되었습니다.")
        else:
            logger.error("푸시 실패. 네트워크 상태나 권한을 확인하세요.")
    else:
        logger.info("ℹ️ 변경된 데이터가 없어 커밋을 스킵합니다.")

if __name__ == "__main__":
    main()
