from pathlib import Path
import csv
import logging
from datetime import datetime


logger = logging.getLogger(__name__)


def get_sra_lists(sra_lists_dir: Path) -> list[Path]:
    return list(sra_lists_dir.glob("*.txt"))


class LogManager:
    def __init__(self, csv_log_dir: Path, python_log_dir: Path):
        self.csv_log_dir = csv_log_dir
        self.python_log_dir = python_log_dir

    def generate_csv_log(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.csv_log_dir / f"progress_{timestamp}.csv"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with log_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Accession", "Download Status", "Validation Status", "Source File"])

        logger.info(f"Created new CSV log file: {log_path}")
        return log_path

    def generate_python_log(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.python_log_dir / f"pipeline_{timestamp}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        return log_path

    def write_csv_log(self, results: list[list[str]], log_path: Path):
        with log_path.open("a", newline="") as log:
            writer = csv.writer(log)
            writer.writerows(results)