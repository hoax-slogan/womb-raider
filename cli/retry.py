from pathlib import Path
import typer

from .cli_components import create_pipeline_components

from ..config import Config
from ..job_orchestrator import SRAOrchestrator
from ..log_manager import LogManager
from ..log_setup import setup_logging

retry = typer.Typer(help="Retry failed accessions from previous pipeline run")


@retry.command("run")
def retry_failed(
    config_file: Path = typer.Option("config.yaml", help="Path to config file"),
    batch_size: int = typer.Option(None),
    threads: int = typer.Option(None),
    max_retries: int = typer.Option(None),
):
    """
    Retry all failed accessions found in the most recent CSV log.
    """
    config = Config(config_file)
    config.ensure_directories_exist()

    log_manager = LogManager(config.CSV_LOG_DIR, config.PYTHON_LOG_DIR)
    python_log_path = log_manager.generate_python_log()
    setup_logging(python_log_path)

    overrides = {
        "batch_size": batch_size,
        "threads": threads,
        "max_retries": max_retries,
        # no processing toggles — just retry failures
    }

    components = create_pipeline_components(config, overrides)
    orchestrator = SRAOrchestrator(**components)
    orchestrator.retry_failed()