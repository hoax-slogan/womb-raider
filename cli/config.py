from pathlib import Path
import typer

from ..config import Config


app = typer.Typer(help="Setup or manage Womb Raider configuration and folders.")


@app.command("init")
def initialize_config(
    config_file: Path = typer.Option("config.yaml", help="Path to config YAML."),
):
    """
    Initialize folder structure and setup logging system based on config.yaml.
    """
    typer.echo("Initializing configuration...")

    config = Config(config_file=config_file, base_dir=Path("."), safe=True, setup_logs=True)

    typer.echo("Configuration and folder structure ready!")