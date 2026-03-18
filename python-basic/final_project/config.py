"""Конфигурация проекта: API, БД, email, Google Sheets."""

# ── API ──────────────────────────────────────────────────────────────
API_URL = "https://b2b.itresume.ru/api/statistics"
API_CLIENT = "Skillfactory"
API_CLIENT_KEY = "M2MGWS"

# ── PostgreSQL ───────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "grader",
    "user": "postgres",
    "password": "postgres",
}

TABLE_NAME = "attempts"

# ── Логирование ──────────────────────────────────────────────────────
LOG_DIR = "logs"
LOG_RETENTION_DAYS = 3
LOG_FILE = f"{LOG_DIR}/grader.log"

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s | %(levelname)-7s | %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
        "file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "formatter": "standard",
            "filename": LOG_FILE,
            "when": "midnight",
            "backupCount": LOG_RETENTION_DAYS,
            "encoding": "utf-8",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console", "file"],
    },
}

# ── Email (задание **) ──────────────────────────────────────────────
SMTP_SERVER = "smtp.mail.ru"
SMTP_PORT = 465
SENDER_EMAIL = "your_email@mail.ru"
SENDER_PASSWORD = "your_app_password"
RECIPIENT_EMAIL = "recipient@example.com"

# ── Google Sheets (задание *) ────────────────────────────────────────
GOOGLE_CREDENTIALS_FILE = "credentials.json"
SPREADSHEET_NAME = "Grader Daily Report"
