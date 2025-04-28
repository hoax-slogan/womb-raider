from pathlib import Path
import csv
import logging

from datetime import datetime
from .db.models import StepStatus

from .constants import CSV_HEADER


logger = logging.getLogger(__name__)


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
            writer.writerow(CSV_HEADER)

        logger.info(f"Created new CSV log file: {log_path}")
        return log_path


    def generate_python_log(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.python_log_dir / f"pipeline_{timestamp}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        return log_path
    

    def get_latest_csv_log(self) -> Path:
        """Return the most recently modified CSV log file, or None if none found."""
        csv_files = list(self.csv_log_dir.glob("*.csv"))
        if not csv_files:
            return None
        return max(csv_files, key=lambda f: f.stat().st_mtime)


    def write_csv_log(self, results: list[list[str]], log_path: Path) -> None:
        with log_path.open("a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(results)
    

    def load_accessions_from_file(self, file_path: Path) -> list[str]:
        with file_path.open("r") as f:
            return [line.strip() for line in f if line.strip()]
    

    def get_failed_accessions(self, log_path: Path) -> list[str]:
        if not log_path.exists():
            logger.warning(f"Log file does not exist: {log_path}")
            return []
        
        failed = []
        with log_path.open("r") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                statuses = row[1:6]  # All step statuses
                if any(status == StepStatus.FAILED.value for status in statuses):
                    failed.append(row[0])
        return failed