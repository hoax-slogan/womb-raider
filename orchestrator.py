from multiprocessing import Pool
from tqdm import tqdm
from typing import Tuple, Callable, Iterable, Any
from .job import Job
import logging
from .log_manager import LogManager
from .utils import get_sra_lists


class SRAOrchestrator:
    def __init__(self, *, output_dir, sra_lists_dir, csv_log_path, fastq_file_dir,
                log_manager, validator, status_checker,
                convert_fastq=False, fastq_threads=4, max_retries=5, batch_size=5,
                s3_handler=None):

        self.output_dir = output_dir
        self.sra_lists_dir = sra_lists_dir
        self.csv_log_path = csv_log_path
        self.fastq_file_dir = fastq_file_dir
        self.log_manager = log_manager
        self.validator = validator
        self.status_checker = status_checker
        self.convert_fastq = convert_fastq
        self.fastq_threads = fastq_threads
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.s3_handler = s3_handler
        self.logger = logging.getLogger(__name__)


    def create_job(self, accession: str, source_file: str) -> Job:
        return Job(
            accession=accession,
            source_file=source_file,
            output_dir=self.output_dir,
            validator=self.validator,
            status_checker=self.status_checker,
            fastq_converter=self._get_fastq_converter(),
            s3_handler=self.s3_handler
        )


    def _get_fastq_converter(self):
        if self.convert_fastq:
            from .fastq_converter import FASTQConverter
            return FASTQConverter(self.fastq_file_dir, self.fastq_threads)
        return None


    def execute_job(self, args: Tuple[str, str]):
        job = self.create_job(*args)
        job.run_download()
        job.run_validation()
        job.run_conversion()
        # Upload logic could go here later if needed
        return [
            job.accession,
            job.status.download.value,
            job.run_validation(),
            job.source_file,
        ]
    

    def process_batch(self, func: Callable[[Any], Any], args: Iterable[Any]) -> list[Any]:
        with Pool(self.batch_size) as pool:
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
        args = [(acc, "retry") for acc in failed]
        results = self.process_batch(self.execute_job, args)

        self.log_manager.write_csv_log(results, self.csv_log_path)