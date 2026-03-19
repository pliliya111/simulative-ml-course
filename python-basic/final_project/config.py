from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_url: str = "https://b2b.itresume.ru/api/statistics"
    api_client: str = "Skillfactory"
    api_client_key: str = "M2MGWS"

    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "grader"
    db_user: str = "postgres"
    db_password: str = "postgres"
    table_name: str = "attempts"

    log_dir: str = "logs"
    log_retention_days: int = 3

    smtp_server: str = "smtp.mail.ru"
    smtp_port: int = 465
    sender_email: str = "your_email@mail.ru"
    sender_password: str = "your_app_password"
    recipient_email: str = "recipient@example.com"

    google_credentials_file: str = "credentials.json"
    spreadsheet_name: str = "Grader Daily Report"

    @property
    def db_config(self) -> dict:
        return {
            "host": self.db_host,
            "port": self.db_port,
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
        }

    @property
    def log_file(self) -> str:
        return f"{self.log_dir}/grader.log"

    @property
    def logging_config(self) -> dict:
        return {
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
                    "filename": self.log_file,
                    "when": "midnight",
                    "backupCount": self.log_retention_days,
                    "encoding": "utf-8",
                },
            },
            "root": {
                "level": "INFO",
                "handlers": ["console", "file"],
            },
        }


settings = Settings()
