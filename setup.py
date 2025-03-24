import os
import logging
from logging.handlers import RotatingFileHandler
from config import SRA_LISTS_DIR, SRA_OUTPUT_DIR, LOGS_DIR, FASTQ_DIR, LOG_FILES


def check_and_make_dirs():
    for path in [SRA_LISTS_DIR, SRA_OUTPUT_DIR, FASTQ_DIR, LOGS_DIR]:
        os.makedirs(path, exist_ok=True)


def setup_logging():
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)

    file_handler = RotatingFileHandler(LOG_FILES, maxBytes=5_000_000, backupCount=3)
    file_handler.setLevel(logging.INFO)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[console, file_handler]
    )

    logging.getLogger().info("Logging system initialized.")