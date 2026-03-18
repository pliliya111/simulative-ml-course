"""
Итоговый проект: ETL-скрипт загрузки данных грейдера в PostgreSQL.

Запуск:
    python main.py                          — данные за вчерашний день
    python main.py 2024-01-01 2024-01-02    — данные за указанный период
"""

import ast
import logging
import logging.config
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values

from config import (
    API_CLIENT,
    API_CLIENT_KEY,
    API_URL,
    DB_CONFIG,
    LOG_DIR,
    LOGGING_CONFIG,
    TABLE_NAME,
)

logger = logging.getLogger(__name__)

VALID_ATTEMPT_TYPES = {"run", "submit"}

INIT_DB_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id                      SERIAL PRIMARY KEY,
    user_id                 VARCHAR(255)  NOT NULL,
    oauth_consumer_key      VARCHAR(255),
    lis_result_sourcedid    TEXT          NOT NULL,
    lis_outcome_service_url TEXT          NOT NULL,
    is_correct              BOOLEAN,
    attempt_type            VARCHAR(50)   NOT NULL,
    created_at              TIMESTAMP     NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_{TABLE_NAME}_dedup
    ON {TABLE_NAME} (user_id, lis_result_sourcedid, created_at);
"""

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME}
    (user_id, oauth_consumer_key, lis_result_sourcedid,
     lis_outcome_service_url, is_correct, attempt_type, created_at)
VALUES %s
ON CONFLICT (user_id, lis_result_sourcedid, created_at) DO NOTHING
"""


# ── Логирование ─────────────────────────────────────────────────────

def setup_logging() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("Логирование инициализировано")


# ── API ──────────────────────────────────────────────────────────────

def fetch_attempts(start: datetime, end: datetime) -> list[dict]:
    params = {
        "client": API_CLIENT,
        "client_key": API_CLIENT_KEY,
        "start": start.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "end": end.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }

    logger.info("Скачивание данных: %s → %s", params["start"], params["end"])

    try:
        response = requests.get(API_URL, params=params, timeout=120)
    except requests.RequestException as exc:
        logger.error("Ошибка соединения с API: %s", exc)
        raise

    if response.status_code != 200:
        logger.error("API вернул status_code=%d: %s", response.status_code, response.text[:300])
        raise RuntimeError(f"API вернул status_code={response.status_code}")

    data = response.json()
    logger.info("Скачивание завершено: %d записей", len(data))
    return data


# ── Обработка и валидация ────────────────────────────────────────────

def _parse_passback_params(raw: str) -> dict | None:
    try:
        return ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        pass
    pairs = re.findall(r"'(\w+)':\s*'([^']*)'", raw)
    return dict(pairs) if pairs else None


def _validate_record(record: dict) -> tuple[dict | None, str | None]:
    """Возвращает (результат, причина_пропуска). Если валидно — причина None."""
    user_id = record.get("lti_user_id")
    if not isinstance(user_id, str) or not user_id.strip():
        return None, "некорректный lti_user_id"

    attempt_type = record.get("attempt_type")
    if attempt_type not in VALID_ATTEMPT_TYPES:
        return None, f"неизвестный attempt_type={attempt_type!r}"

    created_at_raw = record.get("created_at")
    try:
        created_at = datetime.strptime(created_at_raw, "%Y-%m-%d %H:%M:%S.%f")
    except (TypeError, ValueError):
        return None, "некорректный created_at"

    is_correct = record.get("is_correct")
    if is_correct is not None and not isinstance(is_correct, bool):
        if is_correct in (0, 1):
            is_correct = bool(is_correct)
        else:
            return None, "некорректный is_correct"

    raw_params = record.get("passback_params")
    if not raw_params:
        return None, "passback_params отсутствует"

    params = _parse_passback_params(raw_params)
    if params is None:
        return None, "не удалось распарсить passback_params"

    lis_result_sourcedid = params.get("lis_result_sourcedid")
    lis_outcome_service_url = params.get("lis_outcome_service_url")

    if lis_result_sourcedid is None or lis_outcome_service_url is None:
        return None, "отсутствует lis_outcome_service_url в passback_params"

    return {
        "user_id": user_id.strip(),
        "oauth_consumer_key": (params.get("oauth_consumer_key") or "").strip(),
        "lis_result_sourcedid": lis_result_sourcedid.strip(),
        "lis_outcome_service_url": lis_outcome_service_url.strip(),
        "is_correct": is_correct,
        "attempt_type": attempt_type,
        "created_at": created_at,
    }, None


def process_records(raw_records: list[dict]) -> list[dict]:
    logger.info("Обработка данных: %d записей", len(raw_records))
    valid = []
    skip_reasons: Counter[str] = Counter()

    for record in raw_records:
        result, reason = _validate_record(record)
        if result is not None:
            valid.append(result)
        else:
            skip_reasons[reason] += 1

    skipped = sum(skip_reasons.values())
    logger.info("Обработка завершена: валидных %d, пропущено %d", len(valid), skipped)
    for reason, count in skip_reasons.most_common():
        logger.warning("  — %s: %d записей", reason, count)

    return valid


# ── PostgreSQL ───────────────────────────────────────────────────────

def load_to_db(records: list[dict]) -> int:
    if not records:
        logger.info("Нет записей для вставки")
        return 0

    logger.info("Подключение к БД %s@%s:%s/%s",
                DB_CONFIG["user"], DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute(INIT_DB_SQL)
        conn.commit()
        logger.info("Таблица '%s' готова", TABLE_NAME)

        values = [
            (r["user_id"], r["oauth_consumer_key"], r["lis_result_sourcedid"],
             r["lis_outcome_service_url"], r["is_correct"], r["attempt_type"], r["created_at"])
            for r in records
        ]

        logger.info("Загрузка в БД: %d записей", len(values))
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, values, page_size=1000)
            inserted = cur.rowcount
        conn.commit()
        logger.info("Загружено: %d новых (дубли пропущены)", inserted)
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("Соединение с БД закрыто")


# ── Точка входа ──────────────────────────────────────────────────────

def _parse_dates(args: list[str]) -> tuple[datetime, datetime]:
    if len(args) >= 3:
        start = datetime.strptime(args[1], "%Y-%m-%d")
        end = datetime.strptime(args[2], "%Y-%m-%d")
    else:
        end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
    return start, end


def _day_range(start: datetime, end: datetime):
    """Генерирует пары (day_start, day_end) для каждого дня в периоде."""
    day = start
    while day < end:
        next_day = day + timedelta(days=1)
        yield day, min(next_day, end)
        day = next_day


def main() -> None:
    setup_logging()
    logger.info("=" * 60)
    logger.info("Запуск скрипта загрузки данных грейдера")
    logger.info("=" * 60)

    has_errors = False
    start, end = _parse_dates(sys.argv)
    days = list(_day_range(start, end))
    logger.info("Период: %s → %s (%d дн.)", start.date(), end.date(), len(days))

    total_raw = 0
    total_loaded = 0

    for i, (day_start, day_end) in enumerate(days, 1):
        logger.info("── День %d/%d: %s ──", i, len(days), day_start.date())

        # 1. Получение данных из API за один день
        try:
            raw_data = fetch_attempts(day_start, day_end)
        except Exception:
            logger.error("Не удалось получить данные за %s — пропуск", day_start.date())
            has_errors = True
            continue

        if not raw_data:
            logger.info("Нет данных за %s", day_start.date())
            continue

        total_raw += len(raw_data)

        # 2. Обработка и валидация
        records = process_records(raw_data)
        if not records:
            logger.warning("После валидации не осталось записей за %s", day_start.date())
            continue

        # 3. Вставка пачкой в PostgreSQL
        try:
            inserted = load_to_db(records)
            total_loaded += inserted
        except Exception as exc:
            logger.error("Ошибка БД за %s: %s — пропуск", day_start.date(), exc)
            has_errors = True

    logger.info("── Итого: получено %d, загружено %d ──", total_raw, total_loaded)

    # 4. Google Sheets (опционально)
    try:
        from google_sheets import export_daily_stats
        export_daily_stats(records, start)
    except Exception as exc:
        logger.warning("Google Sheets: %s", exc)
        has_errors = True

    # 5. Email (опционально)
    try:
        from email_notifier import send_report
        send_report(total_raw, total_loaded, start, end)
    except Exception as exc:
        logger.warning("Email: %s", exc)
        has_errors = True

    logger.info("=" * 60)
    if has_errors:
        logger.info("Скрипт завершён с предупреждениями (см. выше)")
    else:
        logger.info("Скрипт завершён успешно")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
