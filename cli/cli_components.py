from typing import Dict, Any
from multiprocessing import Pool as DefaultPool

from ..log_manager import LogManager
from ..validators import SRAValidator
from ..status_checker import DownloadStatusChecker
from ..config import Config


def create_pipeline_components(config: Config, overrides: Dict[str, Any]) -> Dict[str, Any]:
    # CLI overrides
    batch_size = overrides.get("batch_size") or config.batch_size
    threads = overrides.get("threads") or config.threads
    max_retries = overrides.get("max_retries") or config.max_retries

    convert_fastq = overrides.get("convert_fastq", False)
    align_star = overrides.get("align_star", False)
    s3_handler = overrides.get("s3_handler", False)

    s3_bucket = overrides.get("s3_bucket") or config.s3_bucket
    s3_prefix = overrides.get("s3_prefix") or ""

    # STAR-specific
    barcode_whitelist = overrides.get("barcode_whitelist")
    cb_start = overrides.get("cb_start")
    cb_len = overrides.get("cb_len")
    umi_start = overrides.get("umi_start")
    umi_len = overrides.get("umi_len")

    # Standard pipeline objects
    log_manager = LogManager(config.csv_log_dir, config.python_log_dir)
    csv_log_path = log_manager.get_latest_csv_log()

    validator = SRAValidator(config.sra_output_dir)
    status_checker = DownloadStatusChecker(config.sra_output_dir)

    return {
        "output_dir": config.sra_output_dir,
        "sra_lists_dir": config.sra_lists_dir,
        "csv_log_path": csv_log_path,
        "fastq_file_dir": config.fastq_dir,
        "star_genome_dir": config.star_genome_dir,
        "star_output_dir": config.star_output_dir,

        "database_url": config.database_url,

        "log_manager": log_manager,
        "validator": validator,
        "status_checker": status_checker,

        "convert_fastq": convert_fastq,
        "align_star": align_star,

        "s3_handler": s3_handler,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,

        "threads": threads,
        "max_retries": max_retries,
        "batch_size": batch_size,

        "barcode_whitelist": barcode_whitelist,
        "cb_start": cb_start,
        "cb_len": cb_len,
        "umi_start": umi_start,
        "umi_len": umi_len,

        "pool_cls": DefaultPool,
    }