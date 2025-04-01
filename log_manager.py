from pathlib import Path
import csv
import logging
from datetime import datetime
from .db.models import StepStatus


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
            writer.writerow(["Accession", "Download Status", "Validation Status", "Source File"])

        logger.info(f"Created new CSV log file: {log_path}")
        return log_path


    def generate_python_log(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.python_log_dir / f"pipeline_{timestamp}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        return log_path


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
        
        with log_path.open("r") as f:
            reader = csv.reader(f)
            next(reader)
            return [row[0] for row in reader if row[1] == StepStatus.FAILED or row[2] == StepStatus.FAILED]