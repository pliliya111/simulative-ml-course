import argparse
import ast
import logging
import re
from collections import Counter
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config import settings
from state import JsonFileStorage, State
from utils.logging_setup import setup_logging
from utils.models import GraderRecord

logger = logging.getLogger(__name__)

BATCH_SIZE = 100

INIT_DB_SQL = f"""
CREATE TABLE IF NOT EXISTS {settings.table_name} (
    id                      SERIAL PRIMARY KEY,
    user_id                 VARCHAR(255)  NOT NULL,
    oauth_consumer_key      VARCHAR(255),
    lis_result_sourcedid    TEXT          NOT NULL,
    lis_outcome_service_url TEXT          NOT NULL,
    is_correct              BOOLEAN,
    attempt_type            VARCHAR(50)   NOT NULL,
    created_at              TIMESTAMP     NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_{settings.table_name}_dedup
    ON {settings.table_name} (user_id, lis_result_sourcedid, created_at);
"""

INSERT_SQL = f"""
INSERT INTO {settings.table_name}
    (user_id, oauth_consumer_key, lis_result_sourcedid,
     lis_outcome_service_url, is_correct, attempt_type, created_at)
VALUES %s
ON CONFLICT (user_id, lis_result_sourcedid, created_at) DO NOTHING
"""


class Extractor:
    def __init__(self):
        self._params_base = {"client": settings.api_client, "client_key": settings.api_client_key}

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, max=60),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def extract(self, start: datetime, end: datetime) -> list[dict]:
        params = {
            **self._params_base,
            "start": start.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "end": end.strftime("%Y-%m-%d %H:%M:%S.%f"),
        }
        logger.info("Скачивание данных: %s → %s", params["start"], params["end"])

        response = requests.get(settings.api_url, params=params, timeout=120)
        if response.status_code != 200:
            logger.error(
                "API вернул status_code=%d: %s",
                response.status_code,
                response.text[:300],
            )
            raise RuntimeError(f"API вернул status_code={response.status_code}")

        data = response.json()
        logger.info("Скачивание завершено: %d записей", len(data))
        return data


class Transformer:
    VALID_ATTEMPT_TYPES = {"run", "submit"}

    def transform(self, raw_records: list[dict]) -> list[GraderRecord]:
        logger.info("Обработка данных: %d записей", len(raw_records))
        valid: list[GraderRecord] = []
        skip_reasons: Counter[str] = Counter()

        for record in raw_records:
            result, reason = self._validate_record(record)
            if result is not None:
                valid.append(result)
            else:
                skip_reasons[reason] += 1

        skipped = sum(skip_reasons.values())
        logger.info(
            "Обработка завершена: валидных %d, пропущено %d", len(valid), skipped
        )
        for reason, count in skip_reasons.most_common():
            logger.warning("  — %s: %d записей", reason, count)

        return valid

    def _validate_record(
            self, record: dict
    ) -> tuple[GraderRecord | None, str | None]:
        user_id = record.get("lti_user_id")
        if not isinstance(user_id, str) or not user_id.strip():
            return None, "некорректный lti_user_id"

        attempt_type = record.get("attempt_type")
        if attempt_type not in self.VALID_ATTEMPT_TYPES:
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

        params = self._parse_passback_params(raw_params)
        if params is None:
            return None, "не удалось распарсить passback_params"

        lis_result_sourcedid = params.get("lis_result_sourcedid")
        lis_outcome_service_url = params.get("lis_outcome_service_url")

        if lis_result_sourcedid is None or lis_outcome_service_url is None:
            return None, "отсутствует lis_outcome_service_url в passback_params"

        return GraderRecord(
            user_id=user_id.strip(),
            oauth_consumer_key=(params.get("oauth_consumer_key") or "").strip(),
            lis_result_sourcedid=lis_result_sourcedid.strip(),
            lis_outcome_service_url=lis_outcome_service_url.strip(),
            is_correct=is_correct,
            attempt_type=attempt_type,
            created_at=created_at,
        ), None

    @staticmethod
    def _parse_passback_params(raw: str) -> dict | None:
        try:
            return ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            pass
        pairs = re.findall(r"'(\w+)':\s*'([^']*)'", raw)
        return dict(pairs) if pairs else None


class Loader:
    def __init__(self):
        self._conn: psycopg2.extensions.connection | None = None

    def _connect(self) -> None:
        logger.info(
            "Подключение к БД %s@%s:%s/%s",
            settings.db_user,
            settings.db_host,
            settings.db_port,
            settings.db_name,
        )
        self._conn = psycopg2.connect(**settings.db_config)
        with self._conn.cursor() as cur:
            cur.execute(INIT_DB_SQL)
        self._conn.commit()
        logger.info("Таблица '%s' готова", settings.table_name)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, max=60),
        retry=retry_if_exception_type((psycopg2.Error, psycopg2.OperationalError)),
        reraise=True,
    )
    def load(self, records: list[GraderRecord]) -> int:
        if not records:
            logger.info("Нет записей для вставки")
            return 0

        if self._conn is None or self._conn.closed:
            self._connect()

        values = [
            (
                r.user_id,
                r.oauth_consumer_key,
                r.lis_result_sourcedid,
                r.lis_outcome_service_url,
                r.is_correct,
                r.attempt_type,
                r.created_at,
            )
            for r in records
        ]

        logger.info("Загрузка в БД: %d записей", len(values))
        try:
            with self._conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, values, page_size=1000)
                inserted = cur.rowcount
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        logger.info("Загружено: %d новых (дубли пропущены)", inserted)
        return inserted

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Соединение с БД закрыто")


class ETLPipeline:
    def __init__(self):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()
        self.state = State(JsonFileStorage("state.json"))

    def run(self, start: datetime, end: datetime) -> None:
        days = list(self._day_range(start, end))
        logger.info("Период: %s → %s (%d дн.)", start.date(), end.date(), len(days))

        has_errors = False
        total_raw = 0
        total_loaded = 0
        all_records: list[GraderRecord] = []

        for i, (day_start, day_end) in enumerate(days, 1):
            state_key = f"day_{day_start.date().isoformat()}"

            if self.state.get_state(state_key) == "loaded":
                logger.info("День %s уже загружен — пропуск", day_start.date())
                continue

            logger.info("── День %d/%d: %s ──", i, len(days), day_start.date())

            # 1. Extract
            try:
                raw_data = self.extractor.extract(day_start, day_end)
            except Exception:
                logger.error(
                    "Не удалось получить данные за %s — пропуск", day_start.date()
                )
                has_errors = True
                continue

            if not raw_data:
                logger.info("Нет данных за %s", day_start.date())
                self.state.set_state(state_key, "empty")
                continue

            total_raw += len(raw_data)

            # 2. Transform
            records = self.transformer.transform(raw_data)
            all_records.extend(records)

            if not records:
                logger.warning(
                    "После валидации не осталось записей за %s", day_start.date()
                )
                self.state.set_state(state_key, "no_valid")
                continue

            # 3. Load
            try:
                inserted = self.loader.load(records)
                total_loaded += inserted
                self.state.set_state(state_key, "loaded")
            except Exception as exc:
                logger.error(
                    "Ошибка БД за %s: %s — пропуск", day_start.date(), exc
                )
                has_errors = True

        self.loader.close()
        logger.info("── Итого: получено %d, загружено %d ──", total_raw, total_loaded)

        self._optional_google_sheets(all_records, start)
        self._optional_email(total_raw, total_loaded, start, end)

        logger.info("=" * 60)
        if has_errors:
            logger.info("Скрипт завершён с предупреждениями (см. выше)")
        else:
            logger.info("Скрипт завершён успешно")
        logger.info("=" * 60)

    @staticmethod
    def _day_range(start: datetime, end: datetime):
        day = start
        while day < end:
            next_day = day + timedelta(days=1)
            yield day, min(next_day, end)
            day = next_day

    @staticmethod
    def _optional_google_sheets(records: list[GraderRecord], start: datetime) -> None:
        try:
            from services.google_sheets import export_daily_stats

            export_daily_stats(records, start)
        except Exception as exc:
            logger.warning("Google Sheets: %s", exc)

    @staticmethod
    def _optional_email(
            total_raw: int, total_loaded: int, start: datetime, end: datetime
    ) -> None:
        try:
            from services.email_notifier import send_report

            send_report(total_raw, total_loaded, start, end)
        except Exception as exc:
            logger.warning("Email: %s", exc)


def _parse_args() -> tuple[datetime, datetime]:
    parser = argparse.ArgumentParser(
        description="ETL-скрипт загрузки данных грейдера в PostgreSQL",
    )
    parser.add_argument("start", nargs="?", help="начало периода (YYYY-MM-DD)")
    parser.add_argument("end", nargs="?", help="конец периода (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.start and args.end:
        return (
            datetime.strptime(args.start, "%Y-%m-%d"),
            datetime.strptime(args.end, "%Y-%m-%d"),
        )

    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return end - timedelta(days=1), end


def main() -> None:
    setup_logging()
    logger.info("=" * 60)
    logger.info("Запуск скрипта загрузки данных грейдера")
    logger.info("=" * 60)

    start, end = _parse_args()
    pipeline = ETLPipeline()
    pipeline.run(start, end)


if __name__ == "__main__":
    main()
