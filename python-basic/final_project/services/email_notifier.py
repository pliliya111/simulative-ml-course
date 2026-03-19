import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage

from config import settings


def send_report(total_raw: int, total_valid: int, start: datetime, end: datetime) -> None:
    subject = f"Grader ETL Report — {start:%Y-%m-%d}"
    body = (
        f"Отчёт о загрузке данных грейдера\n"
        f"{'=' * 40}\n\n"
        f"Период:            {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}\n"
        f"Получено из API:   {total_raw}\n"
        f"Загружено в БД:    {total_valid}\n"
        f"Отброшено:         {total_raw - total_valid}\n\n"
        f"Время отправки:    {datetime.now():%Y-%m-%d %H:%M:%S}\n"
    )

    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = settings.sender_email
    msg["To"] = settings.recipient_email

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(settings.smtp_server, settings.smtp_port, context=context) as server:
        server.login(settings.sender_email, settings.sender_password)
        server.send_message(msg)
