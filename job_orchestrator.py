from multiprocessing import Pool as DefaultPool
from tqdm import tqdm
from typing import Tuple, Callable, Iterable, Any
from pathlib import Path
import logging

from .job_runner import JobRunner
from .fastq_converter import FASTQConverter
from .star_runner import STARRunner
from .s3_handler import S3Handler
from .utils import get_sra_lists


class SRAOrchestrator:
    def __init__(self, *, output_dir: Path, sra_lists_dir: Path, csv_log_path: Path,
                fastq_file_dir: Path, genome_dir: Path, star_output: Path,
                log_manager, validator, status_checker, manifest_manager,
                convert_fastq=False, align_star=False, s3_handler=False,
                s3_bucket=None, s3_prefix="", threads=4, max_retries=5, batch_size=5,
                pool_cls=DefaultPool, barcode_whitelist: Path = None,
                cb_start: int = None, cb_len: int = None, umi_start: int = None,
                umi_len: int = None,):

        self.output_dir = output_dir
        self.sra_lists_dir = sra_lists_dir
        self.genome_dir = genome_dir
        self.star_output = star_output
        self.csv_log_path = csv_log_path
        self.fastq_file_dir = fastq_file_dir
        self.log_manager = log_manager
        self.validator = validator
        self.status_checker = status_checker
        self.manifest_manager = manifest_manager
        self.convert_fastq = convert_fastq
        self.align_star = align_star
        self.s3_handler = s3_handler
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.threads = threads
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.pool_cls = pool_cls
        self.barcode_whitelist = barcode_whitelist
        self.cb_start = cb_start
        self.cb_len = cb_len
        self.umi_start = umi_start
        self.umi_len = umi_len
        self.logger = logging.getLogger(__name__)


    def _get_fastq_converter(self):
        """Returns a FASTQConverter instance if conversion is enabled, else None."""
        if not self.convert_fastq:
            return None
        return FASTQConverter(output_dir=self.fastq_file_dir, threads=self.threads)

    
    def _get_star_runner(self):
        """Returns a STARRunner instance if alignment enabled, else None."""
        if not self.align_star:
            return None
        return STARRunner(
            genome_dir=self.genome_dir,
            star_output=self.star_output,
            barcode_whitelist=self.barcode_whitelist,
            threads=self.threads,
            cb_start=self.cb_start,
            cb_len=self.cb_len,
            umi_start=self.umi_start,
            umi_len=self.umi_len
        )


    def _get_s3_handler(self):
        """Returns a S3Handler instance if AWS is enabled, else None."""
        if not self.s3_handler:
            return None
        if not self.s3_bucket:
            raise ValueError("S3 usage enabled but no bucket name provided.")
        return S3Handler(self.s3_bucket, self.s3_prefix)


    def execute_job(self, args: Tuple[str, str]):
        accession, source_file = args

        runner = JobRunner(
            output_dir=self.output_dir,
            manifest_manager=self.manifest_manager,
            validator=self.validator,
            status_checker=self.status_checker,
            s3_handler=self._get_s3_handler(),
            fastq_converter=self._get_fastq_converter(),
            star_runner=self._get_star_runner(),
            logger=self.logger
        )

        return runner.run(accession, source_file)
    

    def prepare_for_run(self):
        self.csv_log_path = self.log_manager.generate_csv_log()


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