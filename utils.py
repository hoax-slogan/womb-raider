import os
import csv
import logging
import datetime
from config import SRA_LISTS_DIR, PYTHON_LOG_DIR


logger = logging.getLogger(__name__)


def get_sra_lists():
    return sorted([
        os.path.join(SRA_LISTS_DIR, f)
        for f in os.listdir(SRA_LISTS_DIR)
        if f.endswith(".txt")
    ])


def generate_timestamped_csv_log(csv_log_dir):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(csv_log_dir, f"progress_{timestamp}.csv")

    with open(log_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Accession", "Download Status", "Validation Status", "Source File"])

    logger.info(f"Created new CSV log file: {log_path}")
    return log_path


def generate_timestamped_logfile():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(PYTHON_LOG_DIR, f"pipeline_{timestamp}.log")


def write_log(results, log_path):
    """
    Write download results to the log file.
    """
    with open(log_path, "a", newline="") as log:
        writer = csv.writer(log)
        writer.writerows(results)