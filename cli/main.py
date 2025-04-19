import typer
from cli import download, retry


cli = typer.Typer(help="Womb Raider pipeline CLI")

# Mount the download command group
cli.add_typer(download.download, name="download")
cli.add_typer(retry.retry, name="retry")
