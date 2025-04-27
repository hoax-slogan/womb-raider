from pathlib import Path
import typer

from .cli_components import create_pipeline_components
from ..config import Config
from ..job_orchestrator import SRAOrchestrator


app = typer.Typer(help="Align FASTQ files with STAR.")


@app.command("run")
def align_run(
    config_file: Path = typer.Option("config.yaml", help="Path to config file."),
    batch_size: int = typer.Option(None, help="Max jobs per batch."),
    threads: int = typer.Option(None, help="Threads for STAR."),
    cb_start: int = typer.Option(None, help="Cell barcode start pos."),
    cb_len: int = typer.Option(None, help="Cell barcode length."),
    umi_start: int = typer.Option(None, help="UMI start pos."),
    umi_len: int = typer.Option(None, help="UMI length."),
    barcode_whitelist: Path = typer.Option(None, help="Path to whitelist."),
    fresh_run: bool = typer.Option(False, help="Initialize a new CSV log?"),
):
    """
    Align converted FASTQ files to a genome reference using STAR.
    """
    config = Config(config_file=config_file, safe=False, setup_logs=False)

    overrides = {
        "batch_size": batch_size,
        "threads": threads,
        "max_retries": None,
        "convert_fastq": False,   # DO NOT run fasterq
        "align_star": True,       # ONLY align
        "s3_handler": False,      # NO S3 upload unless specifically needed
        "barcode_whitelist": barcode_whitelist,
        "cb_start": cb_start,
        "cb_len": cb_len,
        "umi_start": umi_start,
        "umi_len": umi_len,
    }

    components = create_pipeline_components(config, overrides)
    orchestrator = SRAOrchestrator(**components)

    if fresh_run:
        orchestrator.prepare_for_run()
    
    orchestrator.retry_failed()