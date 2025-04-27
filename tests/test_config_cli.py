import os
import pytest
from typer.testing import CliRunner
from rich import print as rprint
from ..cli.cli import cli


runner = CliRunner()


@pytest.fixture
def temp_cwd(tmp_path):
    """Temporarily change to a temp directory during a test."""
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        yield tmp_path
    finally:
        os.chdir(old_cwd)


def test_config_init(temp_cwd):
    """
    Test 'womb config init' creates the expected directory structure.
    """
    config_content = """
    data_dir: sra_data

    subdirs:
        lists: sra_lists
        output: sra_files
        logs: logs
        fastq: fastq_files
        star: star

    logs:
        csv: csv_logs
        python: python_logs

    star:
        genome_dir: genome_ref
        star_output: star_output
    """
    config_file = temp_cwd / "config.yaml"
    config_file.write_text(config_content)

    result = runner.invoke(
        cli,
        ["config", "init", "--config-file", str(config_file)],
        catch_exceptions=False,
    )

    # If CLI fails, dump colored debug info
    if result.exit_code != 0:
        rprint(f"[bold red]--- STDOUT ---[/bold red]\n{result.stdout}")
        rprint(f"[bold red]--- STDERR ---[/bold red]\n{result.stderr}")
    assert result.exit_code == 0, "CLI command failed"

    data_root = temp_cwd / "sra_data"
    expected_subdirs = [
        "sra_lists",
        "sra_files",
        "logs/csv_logs",
        "logs/python_logs",
        "fastq_files",
        "star/genome_ref",
        "star/star_output",
    ]

    for subdir in expected_subdirs:
        path = data_root / subdir
        assert path.is_dir(), f"Missing expected directory: {path}"