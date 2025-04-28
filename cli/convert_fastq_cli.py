from pathlib import Path
import typer

from ..config import Config
from ..job_orchestrator import SRAOrchestrator
from .cli_components import create_pipeline_components


app = typer.Typer(help="Convert SRA files to FASTQ using fasterq-dump.")


@app.command("run")
def convert_fastq(
    config_file: Path = typer.Option("config.yaml", help="Path to config file."),
    batch_size: int = typer.Option(None, help="Max jobs per batch."),
    threads: int = typer.Option(None, help="Threads per job (for fasterq-dump)."),
    fresh_run: bool = typer.Option(False, help="Initialize a new CSV log?"),
):
    """
    Convert SRA files to FASTQ format.
    """
    config = Config(config_file=config_file, safe=False, setup_logs=False)

    overrides = {
        "batch_size": batch_size,
        "threads": threads,
        "convert_fastq": True,  # Force only conversion enabled
        "align_star": False,
        "s3_handler": False,
    }

    components = create_pipeline_components(config, overrides)
    orchestrator = SRAOrchestrator(**components)

    if fresh_run:
        orchestrator.prepare_for_run()

    orchestrator.process_sra_lists()
    orchestrator.retry_failed()