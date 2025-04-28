import typer
from . import align_star_cli, config_cli, convert_fastq_cli, download_cli, s3_upload_cli


cli = typer.Typer(help="Womb Raider: SRA Processing Pipeline")


cli.add_typer(config_cli.app, name="config")
cli.add_typer(download_cli.app, name="download")
cli.add_typer(convert_fastq_cli.app, name="convert-fastq")
cli.add_typer(align_star_cli.app, name="align-star")
cli.add_typer(s3_upload_cli.app, name="upload-s3")
