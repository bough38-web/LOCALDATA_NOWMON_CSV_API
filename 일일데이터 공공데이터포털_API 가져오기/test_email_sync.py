import sys
from pathlib import Path

# Setup path logic (identical to daily_fetch.py)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

print(f"Project Root: {PROJECT_ROOT}")

try:
    from src.notifier import EmailNotifier
    print("✅ Successfully imported EmailNotifier")
    
    notifier = EmailNotifier()
    print(f"Sender: {notifier.sender_email}")
    print(f"Receiver: {notifier.receiver_email}")
    
    test_msg = """[이메일 연동 테스트]
가상 수집 결과 리포트입니다.
완료 시각: 2026-04-02 07:25:00
발견 건수: 1,234건
    """
    
    success = notifier.send_sync_report(test_msg)
    if success:
        print("✅ Test email sent successfully!")
    else:
        print("❌ Failed to send test email. Check credentials in secrets.toml.")
        
except ImportError as e:
    print(f"❌ Import error: {e}")
except Exception as e:
    print(f"❌ Error: {e}")
