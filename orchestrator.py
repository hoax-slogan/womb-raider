from multiprocessing import Pool as DefaultPool
from tqdm import tqdm
from typing import Tuple, Callable, Iterable, Any
from pathlib import Path
import logging

from .db.session import SessionLocal
from .manifest_manager import ManifestManager

from .job import Job
from .utils import get_sra_lists


class SRAOrchestrator:
    def __init__(self, *, output_dir: Path,sra_lists_dir: Path, csv_log_path: Path,
                fastq_file_dir: Path, log_manager, validator, status_checker, manifest_manager,
                convert_fastq=False, fastq_threads=4, max_retries=5, batch_size=5,
                s3_handler=None,  cleanup_local=False, pool_cls=DefaultPool):

        self.output_dir = output_dir
        self.sra_lists_dir = sra_lists_dir
        self.csv_log_path = csv_log_path
        self.fastq_file_dir = fastq_file_dir
        self.log_manager = log_manager
        self.validator = validator
        self.status_checker = status_checker
        self.manifest_manager = manifest_manager
        self.convert_fastq = convert_fastq
        self.fastq_threads = fastq_threads
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.s3_handler = s3_handler
        self.cleanup_local = cleanup_local
        self.pool_cls = pool_cls
        self.logger = logging.getLogger(__name__)


    def create_job(self, accession: str, source_file: str, manifest_manager=None) -> Job:
        return Job(
            accession=accession,
            source_file=source_file,
            output_dir=self.output_dir,
            validator=self.validator,
            status_checker=self.status_checker,
            manifest_manager=manifest_manager or self.manifest_manager,
            fastq_converter=self._get_fastq_converter(),
            s3_handler=self.s3_handler
        )


    def _get_fastq_converter(self):
        if self.convert_fastq:
            from .fastq_converter import FASTQConverter
            return FASTQConverter(self.fastq_file_dir, self.fastq_threads)
        return None


    def execute_job(self, args: Tuple[str, str]):
        accession, source_file = args
        fastq_files = []
        
        # create local orm session per job executed
        # so no anoying detachedinstance error
        session = SessionLocal()
        try:
            manifest_manager = ManifestManager(session)
            job = self.create_job(
                accession=accession,
                source_file=source_file,
                manifest_manager=manifest_manager
            )

            download_ok = job.run_download()
            job.run_validation()

            # if download successful + convert fastq flag = true
            if download_ok and self.convert_fastq:
                fastq_files = job.run_conversion()
            
            # if s3 handler flagged and fast_q files converted
            if self.s3_handler and fastq_files:
                for file in fastq_files:
                    job.run_upload(file)
                    self.logger.info(f"Uploaded {file.name} to S3")
            
            # clean those cups and those spoons
            if self.cleanup_local:
                self._cleanup_files(accession, fastq_files)
                    

            # Extract plain log row BEFORE closing the session
            return job.to_log_row()

        finally:
            session.close()


    def process_batch(self, func: Callable[[Any], Any], args: Iterable[Any]) -> list[Any]:
        with self.pool_cls(self.batch_size) as pool:
            return list(tqdm(pool.imap(func, args), total=len(args)))


    def process_sra_lists(self):
        for sra_file in get_sra_lists(self.sra_lists_dir):
            accessions = self.log_manager.load_accessions_from_file(sra_file)
            self.logger.info(f"Processing {len(accessions)} accessions from {sra_file}")

            args = [(acc, sra_file) for acc in accessions]
            results = self.process_batch(self.execute_job, args)

            self.log_manager.write_csv_log(results, self.csv_log_path)


    def retry_failed(self):
        failed = self.log_manager.get_failed_accessions(self.csv_log_path)

        if not failed:
            self.logger.info("No failed accessions to retry.")
            return

        self.logger.info(f"Retrying {len(failed)} failed accessions...")
        args = [(acc, "Retry") for acc in failed]
        results = self.process_batch(self.execute_job, args)
        self.log_manager.write_csv_log(results, self.csv_log_path)

    
    def _cleanup_files(self, accession: str, fastq_files: list[Path]):
        sra_file = self.output_dir / accession / f"{accession}.sra"
        all_files = fastq_files + [sra_file]

        for file in all_files:
            try:
                if file.exists():
                    file.unlink()
                    self.logger.info(f"Deleted local file: {file}")
                else:
                    self.logger.debug(f"File not found during cleanup: {file}")
            except Exception as e:
                self.logger.warning(f"Failed to delete {file}: {e}")
        
        # scrub directory after files
        self._cleanup_directory(accession)
    

    def _cleanup_directory(self, accession: str):
        accession_dir = self.output_dir / accession

        try:
            if accession_dir.exists() and not any(accession_dir.iterdir()):
                accession_dir.rmdir()
                self.logger.info(f"Removed empty accession folder: {accession_dir}")
            else:
                self.logger.debug(f"Directory not empty, skipping removal: {accession_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to delete folder {accession_dir}: {e}")