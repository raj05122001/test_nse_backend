import logging
import sys
from logging.handlers import RotatingFileHandler


def get_logger(
    name: str = None,
    level: int = logging.INFO,
    log_file: str = None,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """
    Configures and returns a logger with a console handler and optional rotating file handler.

    :param name: Logger name; defaults to root logger if None.
    :param level: Logging level (e.g., logging.INFO).
    :param log_file: Path to log file. If None, file handler is not added.
    :param max_bytes: Max size in bytes before rotating the log file.
    :param backup_count: Number of backup files to keep.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # Formatter for both console and file
    formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Rotating file handler (optional)
    if log_file:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Example usage:
# from app.utils.logger import get_logger
# logger = get_logger(__name__, level=logging.DEBUG, log_file='logs/app.log')
# logger.info("Application started")
