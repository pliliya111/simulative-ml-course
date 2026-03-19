import logging
import logging.config
import os

from config import settings

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    os.makedirs(settings.log_dir, exist_ok=True)
    logging.config.dictConfig(settings.logging_config)
    logger.info("Логирование инициализировано")
