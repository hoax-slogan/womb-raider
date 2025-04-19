from typing import Dict, Any

from ..log_manager import LogManager
from ..validators import SRAValidator
from ..status_checker import DownloadStatusChecker
from ..manifest_manager import ManifestManager

from ..config import Config
from ..db.session import SessionLocal


def create_pipeline_components(config: Config, overrides: Dict[str, Any]) -> Dict[str, Any]:
    # CLI overrides with fallback to config
    batch_size = overrides.get("batch_size") or config.batch_size
    threads = overrides.get("threads") or config.threads
    max_retries = overrides.get("max_retries") or config.max_retries

    convert_fastq = overrides.get("convert_fastq", False)
    align_star = overrides.get("align_star", False)
    s3_handler = overrides.get("s3_handler", False)

    s3_bucket = overrides.get("s3_bucket") or config.s3_bucket
    s3_prefix = overrides.get("s3_prefix") or ""

    # STAR-specific CLI inputs
    barcode_whitelist = overrides.get("barcode_whitelist")
    cb_start = overrides.get("cb_start")
    cb_len = overrides.get("cb_len")
    umi_start = overrides.get("umi_start")
    umi_len = overrides.get("umi_len")

    # Standard pipeline objects
    log_manager = LogManager(config.csv_log_dir, config.python_log_dir)
    validator = SRAValidator(config.output_dir)
    status_checker = DownloadStatusChecker(config.output_dir)
    manifest_manager = ManifestManager(SessionLocal())

    return {
        "output_dir": config.output_dir,
        "sra_lists_dir": config.sra_lists_dir,
        "csv_log_path": config.csv_log_path,
        "fastq_file_dir": config.fastq_dir,
        "genome_dir": config.genome_dir,
        "star_output": config.star_output,

        "log_manager": log_manager,
        "validator": validator,
        "status_checker": status_checker,
        "manifest_manager": manifest_manager,

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

        "pool_cls": None  # Swap with DefaultPool if you start exposing it
    }
