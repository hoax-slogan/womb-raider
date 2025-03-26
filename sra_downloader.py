import time
import subprocess
import logging
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
from validators import SRAValidator
from log_manager import LogManager
from status_checker import DownloadStatusChecker
from utils import get_sra_lists


class SRADownloader:
    def __init__(self, output_dir: Path, sra_lists_dir: Path, csv_log_path: Path,
                log_manager: LogManager, validator: SRAValidator, status_checker: DownloadStatusChecker,
                max_retries=5, batch_size=5):
        
        self.output_dir = output_dir
        self.sra_lists_dir= sra_lists_dir
        self.csv_log_path = csv_log_path
        self.log_manager = log_manager
        self.validator = validator
        self.status_checker = status_checker
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)


    def download(self, accession: str, source_file: str) -> list[str]:

        status = self.status_checker.check_status(accession)
        if status == "Already Exists":
            self.logger.info(f"{accession} already exists, skipping download.")
            validation = self.validator.validate(accession)
            return [accession, status, validation, source_file]

        self.logger.info(f"Downloading {accession}...")

        for attempt in range(1, self.max_retries + 1):
            try:
                cmd = ["prefetch", "--max-size", "100G", "-O", str(self.output_dir), accession]
                subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                # Confirm the download status after successful download
                status = self.status_checker.confirm_download(accession)
                validation = self.validator.validate(accession)
                return [accession, status, validation, source_file]

            except subprocess.CalledProcessError as e:
                self.logger.error(f"{accession} error: {e}")
                self.logger.warning(f"Retry {attempt}/{self.max_retries} for {accession}...")
                time.sleep(5)
        
        status = self.status_checker.confirm_download(accession)
        validation = self.validator.validate(accession)
        return [accession, status, validation, source_file]


    def process_sra_lists(self):
        for sra_file in get_sra_lists(self.sra_lists_dir):
            accessions = self.log_manager.load_accessions_from_file(sra_file)

            self.logger.info(f"Processing {len(accessions)} accessions from {sra_file}")
            with Pool(self.batch_size) as pool:
                results = list(
                    tqdm(pool.imap(lambda acc: self.download(acc, sra_file), accessions), total=len(accessions))
                )

            self.log_manager.write_csv_log(results, self.csv_log_path)
            self.logger.info(f"Logged {len(results)} entries to {self.csv_log_path}")


    def retry_failed(self):
        failed = self.log_manager.get_failed_accessions(self.csv_log_path)

        if not failed:
            self.logger.info("No failed accessions found.")
            return

        self.logger.info(f"Retrying: {failed}")
        with Pool(self.batch_size) as pool:
            results = list(
                tqdm(pool.imap(lambda acc: self.download(acc, "retry"), failed), total=len(failed))
            )

        self.log_manager.write_csv_log(results, self.csv_log_path)
        self.logger.info(f"Logged retries to {self.csv_log_path}")