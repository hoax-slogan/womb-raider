import logging
from logging.handlers import RotatingFileHandler

from utils import generate_timestamped_logfile


def setup_logging():
    log_filename = generate_timestamped_logfile()

    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)

    file_handler = RotatingFileHandler(log_filename, maxBytes=5_000_000, backupCount=3)
    file_handler.setLevel(logging.INFO)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[console, file_handler]
    )

    logging.getLogger().info(f"Logging system initialized: {log_filename}")

    return log_filename