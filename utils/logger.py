import logging
import logging.handlers
import sys
from logging.handlers import RotatingFileHandler

from config.config import LOG_FILE, LOG_LEVEL

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

def get_logger(name):
    logger = logging.getLogger(name)

    level = LOG_LEVELS.get(LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    if not logging.handlers:
        formatter = logging.Formatter(LOG_FORMAT)

        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging. StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
