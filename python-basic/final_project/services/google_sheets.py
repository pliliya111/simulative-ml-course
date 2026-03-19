from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import gspread
from google.oauth2.service_account import Credentials

from config import settings

if TYPE_CHECKING:
    from utils.models import GraderRecord

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADER = ["Дата", "Всего попыток", "Submit-попыток", "Успешных submit", "Уникальных пользователей"]


def export_daily_stats(records: list[GraderRecord], date: datetime) -> None:
    creds = Credentials.from_service_account_file(settings.google_credentials_file, scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(settings.spreadsheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        sheet = client.create(settings.spreadsheet_name).sheet1
        sheet.append_row(HEADER)

    submits = [r for r in records if r.attempt_type == "submit"]
    row = [
        date.strftime("%Y-%m-%d"),
        str(len(records)),
        str(len(submits)),
        str(sum(1 for r in submits if r.is_correct is True)),
        str(len({r.user_id for r in records})),
    ]
    sheet.append_row(row)
