from pathlib import Path
from typing import Optional
import csv
import logging

from datetime import datetime
from ..db.models import StepStatus
from ..config.path_structs import LogPaths

from ..constants import CSV_HEADER


logger = logging.getLogger(__name__)


class LogManager:
    def __init__(self, paths: LogPaths):
        self._logs = paths


    def generate_csv_log(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self._logs.csv_log_dir / f"progress_{timestamp}.csv"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with log_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)

        logger.info(f"Created new CSV log file: {log_path}")
        return log_path


    def generate_python_log(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self._logs.python_log_dir / f"pipeline_{timestamp}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        return log_path
    

    def get_latest_csv_log(self) -> Optional[Path]:
        """Return the most recently modified CSV log file, or None if none found."""
        csv_files = list(self._logs.csv_log_dir.glob("*.csv"))
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
        
    
    def write_unmatched_summary(self, unmatched_barcodes: dict, accession: str) -> None:
        """Write unmatched barcode summary to a file in the given output directory."""

        summary_path = self._logs.split_log_dir / f"{accession}_unmatched_barcodes.txt"
        
        with summary_path.open("w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["Barcode", "Count"])
            for barcode, count in unmatched_barcodes.items():
                writer.writerow([barcode, count])
        logger.info(f"Unmatched barcode summary saved to {summary_path}")
    

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