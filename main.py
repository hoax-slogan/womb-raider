from log_setup import check_and_make_dirs, setup_logging
from utils import ensure_csv_log_exists
import logging

# i WIP my proj back and forth

def main():
    check_and_make_dirs()
    setup_logging()
    ensure_csv_log_exists()

    logger = logging.getLogger(__name__)
    logger.info("CSV log file checked/created.")

if __name__ == "__main__":
    main()