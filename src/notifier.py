import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from datetime import datetime

class EmailNotifier:
    def __init__(self, smtp_server="smtp.gmail.com", smtp_port=587):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        
        # Load from environment variables (Support multiple name variants for platform compatibility)
        self.sender_email = os.environ.get("SENDER_EMAIL") or os.environ.get("EMAIL_SENDER")
        self.app_password = os.environ.get("SENDER_PASSWORD") or os.environ.get("EMAIL_PASSWORD")
        self.receiver_email = os.environ.get("RECEIVER_EMAIL") or os.environ.get("EMAIL_RECEIVER")
        
        # Fallback for cron jobs / terminal execution: parse secrets.toml directly
        if not self.sender_email or not self.app_password:
            self._load_from_secrets()
            
        # Defaults if nowhere to be found
        if not self.sender_email:
            self.sender_email = "your-email@gmail.com"
        if not self.app_password:
            self.app_password = "your-app-password"
        if not self.receiver_email:
            self.receiver_email = self.sender_email
            
    def _load_from_secrets(self):
        """Minimal TOML string parser to read .streamlit/secrets.toml manually."""
        import pathlib
        
        # Compute absolute path to .streamlit/secrets.toml based on root dir
        project_root = pathlib.Path(__file__).parent.parent
        secrets_path = project_root / ".streamlit" / "secrets.toml"
        
        if secrets_path.exists():
            for line in secrets_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("SENDER_EMAIL") and "=" in line:
                    val = line.split("=", 1)[1].strip()
                    self.sender_email = val.strip("'\"")
                elif line.startswith("SENDER_PASSWORD") and "=" in line:
                    val = line.split("=", 1)[1].strip()
                    self.app_password = val.strip("'\"")
                elif line.startswith("RECEIVER_EMAIL") and "=" in line:
                    val = line.split("=", 1)[1].strip()
                    self.receiver_email = val.strip("'\"")

    def send_sync_report(self, summary_text):
        """
        Sends the synchronization summary report via email.
        """
        if not self.sender_email or not self.app_password:
            print("⚠️ Email credentials not found. Skipping email report.")
            return False

        subject = f"📊 [Field Sales Assistant] Daily Sync Report - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Create HTML content for a professional look
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="background-color: #2563eb; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
                <h2 style="margin: 0;">영업기회 데이터 동기화 리포트</h2>
            </div>
            <div style="padding: 20px; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 8px 8px;">
                <p>안녕하세요, 관리자님.</p>
                <p>오늘의 공공데이터 자동 수집 및 동기화가 성공적으로 완료되었습니다.</p>
                
                <div style="background-color: #f3f4f6; padding: 15px; border-radius: 4px; border-left: 4px solid #2563eb;">
                    <pre style="white-space: pre-wrap; font-size: 14px;">{summary_text}</pre>
                </div>
                
                <p style="margin-top: 20px;">상세 내역은 대시보드 인허가 데이터 섹션에서 확인하실 수 있습니다.</p>
                <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 20px 0;">
                <p style="font-size: 12px; color: #6b7280;">이 메일은 시스템에서 자동으로 발송되었습니다.</p>
            </div>
        </body>
        </html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        
        msg.attach(MIMEText(html_content, "html"))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                # Clean up password layout (remove spaces and dashes)
                clean_password = self.app_password.replace("-", "").replace(" ", "")
                server.login(self.sender_email, clean_password)
                
                # 다중 수신자 처리 (콤마로 구분된 경우 리스트로 변환)
                receivers_list = [email.strip() for email in self.receiver_email.split(',')]
                
                server.sendmail(self.sender_email, receivers_list, msg.as_string())
            print(f"✅ Email report sent successfully to: {receivers_list}")
            return True
        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            return False

    def send_progress_report(self, summary_text):
        """
        Sends an intermediate progress report via email.
        """
        if not self.sender_email or not self.app_password:
            return False

        subject = f"⏳ [진행 현황 보고] API 데이터 수집중... - {datetime.now().strftime('%H:%M')}"
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="background-color: #6b7280; color: white; padding: 15px; border-radius: 8px 8px 0 0;">
                <h3 style="margin: 0;">수집 진행 현황 실시간 보고</h3>
            </div>
            <div style="padding: 20px; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 8px 8px;">
                <p>알림 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>현재 데이터 수집이 진행 중이며, 30분 단위 정기 진행 사항을 보고드립니다.</p>
                
                <div style="background-color: #f9fafb; padding: 15px; border-radius: 4px; border-left: 4px solid #6b7280; font-family: monospace;">
                    <pre style="white-space: pre-wrap; font-size: 13px;">{summary_text}</pre>
                </div>
                
                <p style="margin-top: 20px; font-size: 12px; color: #6b7280;">최종 완료 시 상세 리포트가 추가로 발송될 예정입니다.</p>
            </div>
        </body>
        </html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg.attach(MIMEText(html_content, "html"))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                clean_password = self.app_password.replace("-", "").replace(" ", "")
                server.login(self.sender_email, clean_password)
                receivers_list = [email.strip() for email in self.receiver_email.split(',')]
                server.sendmail(self.sender_email, receivers_list, msg.as_string())
            print(f"📡 Progress report sent successfully to: {receivers_list}")
            return True
        except Exception as e:
            print(f"❌ Failed to send progress report: {e}")
            return False

if __name__ == "__main__":
    # Test block
    notifier = EmailNotifier()
    notifier.send_sync_report("This is a test summary from the automated system.")
