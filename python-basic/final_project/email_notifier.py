import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage

from config import (
    RECIPIENT_EMAIL,
    SENDER_EMAIL,
    SENDER_PASSWORD,
    SMTP_PORT,
    SMTP_SERVER,
)


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
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
