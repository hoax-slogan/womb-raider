from pathlib import Path
import typer

from .cli_components import create_pipeline_components

from ..config import Config
from ..job_orchestrator import SRAOrchestrator
from ..log_manager import LogManager
from ..log_setup import setup_logging


download = typer.Typer(help="Run primary SRA download and processing pipeline")


@download.command("run")
def run_pipeline(
    # Config file
    config_file: Path = typer.Option("config.yaml", help="Path to config file"),

    # Core pipeline toggles
    convert_fastq: bool = typer.Option(False, help="Enable FASTQ conversion step"),
    align_star: bool = typer.Option(False, help="Enable STAR alignment step"),
    s3_handler: bool = typer.Option(False, help="Enable S3 upload after processing"),

    # Runtime behavior
    batch_size: int = typer.Option(None, help="Max jobs to process per batch"),
    threads: int = typer.Option(None, help="Number of threads per job"),
    max_retries: int = typer.Option(None, help="Max retries for failed jobs"),

    # S3-related settings
    s3_bucket: str = typer.Option(None, help="S3 bucket name (used if --s3-handler)"),
    s3_prefix: str = typer.Option("", help="Optional S3 object prefix"),

    # STAR-specific alignment settings
    barcode_whitelist: Path = typer.Option(None, help="Path to barcode whitelist file"),
    cb_start: int = typer.Option(None, help="Cell barcode start position"),
    cb_len: int = typer.Option(None, help="Cell barcode length"),
    umi_start: int = typer.Option(None, help="UMI start position"),
    umi_len: int = typer.Option(None, help="UMI length"),
):
    """
    Run the SRA processing pipeline with configurable steps.
    Steps include download, validation, FASTQ conversion, STAR alignment, and S3 upload.
    """
    # Load config and ensure all necessary directories exist
    config = Config(config_file)
    config.ensure_directories_exist()

    # Set up timestamped logging for the run
    log_manager = LogManager(config.CSV_LOG_DIR, config.PYTHON_LOG_DIR)
    python_log_path = log_manager.generate_python_log()
    setup_logging(python_log_path)

    # Bundle CLI overrides into the orchestrator
    overrides = {
        "batch_size": batch_size,
        "threads": threads,
        "max_retries": max_retries,
        "convert_fastq": convert_fastq,
        "align_star": align_star,
        "s3_handler": s3_handler,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "barcode_whitelist": barcode_whitelist,
        "cb_start": cb_start,
        "cb_len": cb_len,
        "umi_start": umi_start,
        "umi_len": umi_len,
    }

    # Orchestrate the pipeline
    pipeline_components = create_pipeline_components(config, overrides)
    orchestrator = SRAOrchestrator(**pipeline_components)
    orchestrator.process_sra_lists()