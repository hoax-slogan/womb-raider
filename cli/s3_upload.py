from pathlib import Path
import typer

from ..config import Config
from ..job_orchestrator import SRAOrchestrator
from .cli_components import create_pipeline_components


app = typer.Typer(help="Upload STAR outputs to S3 bucket.")


@app.command("upload")
def s3_upload(
    config_file: Path = typer.Option("config.yaml", help="Path to config file."),
    batch_size: int = typer.Option(None, help="Max jobs per batch."),
    threads: int = typer.Option(None, help="Threads per job."),
    s3_bucket: str = typer.Option(None, help="S3 bucket name."),
    s3_prefix: str = typer.Option("", help="S3 object prefix (optional)."),
    fresh_run: bool = typer.Option(False, help="Initialize a new CSV log?"),
):
    """
    Upload STAR output files to S3.
    """
    config = Config(config_file=config_file, safe=False, setup_logs=False)

    overrides = {
        "batch_size": batch_size,
        "threads": threads,
        "convert_fastq": False,
        "align_star": False,
        "s3_handler": True,  # <<< Enable S3 handling
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
    }

    components = create_pipeline_components(config, overrides)
    orchestrator = SRAOrchestrator(**components)

    if fresh_run:
        orchestrator.prepare_for_run()
    
    orchestrator.retry_failed()