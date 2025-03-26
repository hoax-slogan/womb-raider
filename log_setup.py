import logging
from logging.handlers import RotatingFileHandler


def setup_logging(log_path):
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)

    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3)
    file_handler.setLevel(logging.INFO)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[console, file_handler]
    )

    logging.getLogger().info(f"Logging system initialized: {log_path}")