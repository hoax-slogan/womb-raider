import typer
from . import config, download, convert_fastq, align_star, s3_upload


cli = typer.Typer(help="Womb Raider: SRA Processing Pipeline")


cli.add_typer(config.app, name="config")
cli.add_typer(download.app, name="download")
cli.add_typer(convert_fastq.app, name="convert-fastq")
cli.add_typer(align_star.app, name="align-star")
cli.add_typer(s3_upload.app, name="upload-s3")
