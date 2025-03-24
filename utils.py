import os
import csv
import logging
from config import SRA_LISTS_DIR, CSV_LOGS


logger = logging.getLogger(__name__)


def get_sra_lists():
    return sorted([
        os.path.join(SRA_LISTS_DIR, f)
        for f in os.listdir(SRA_LISTS_DIR)
        if f.endswith(".txt")
    ])


def ensure_csv_log_exists():
    if not os.path.exists(CSV_LOGS):
        logger.info(f"Creating log file: {CSV_LOGS}")
        with open(CSV_LOGS, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Accession", "Download Status", "Validation Status", "Source File"])


def write_log(results):
    """
    Write download results to the log file.
    """
    with open(CSV_LOGS, "a", newline="") as log:
        writer = csv.writer(log)
        writer.writerows(results)