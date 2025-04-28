from pathlib import Path
import typer

from .cli_components import create_pipeline_components
from ..config import Config
from ..job_orchestrator import SRAOrchestrator


app = typer.Typer(help="Download and validate SRA files.")


@app.command("run")
def download(
    config_file: Path = typer.Option("config.yaml", help="Path to config file."),
    batch_size: int = typer.Option(None, help="Max jobs per batch."),
    threads: int = typer.Option(None, help="Threads per job."),
    max_retries: int = typer.Option(None, help="Max retries for failed downloads."),
    fresh_run: bool = typer.Option(True, help="Initialize a new CSV log?"),
):
    """
    Download and validate SRA files only.
    """
    config = Config(config_file=config_file, safe=False, setup_logs=False)

    overrides = {
        "batch_size": batch_size,
        "threads": threads,
        "max_retries": max_retries,
        "convert_fastq": False,
        "align_star": False,
        "s3_handler": False,
    }

    components = create_pipeline_components(config, overrides)
    orchestrator = SRAOrchestrator(**components)

    if fresh_run:
        orchestrator.prepare_for_run()

    orchestrator.process_sra_lists()
    orchestrator.retry_failed()