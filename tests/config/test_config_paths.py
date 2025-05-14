import pytest

from ...config.paths import ConfigPaths
from ...config.schema import *

@pytest.fixture
def dummy_config():
    return WombRaiderConfig(
        data_dir="base",
        subdirs=SubdirsConfig(
            lists="lists",
            output="output",
            logs="logs",
            fastq="fastq",
            split_fastq="split_fastq",
            barcodes="barcodes",
            star="star"
        ),
        logs=LogsConfig(
            csv="csv_logs",
            python="python_logs",
            split="split_logs"
        ),
        star=StarConfig(
            genome_dir="genome",
            star_output="star_out"
        ),
        batch_size=5,
        threads=4,
        max_retries=3,
        s3_bucket=None,
        s3_prefix=""
    )


def test_config_paths_resolution(dummy_config, tmp_path):
    paths = ConfigPaths(config=dummy_config, base_dir=tmp_path)

    assert paths.data_dir == tmp_path / "base"
    assert paths.sra_lists_dir == tmp_path / "base" / "lists"
    assert paths.sra_output_dir == tmp_path / "base" / "output"
    assert paths.logs_dir == tmp_path / "base" / "logs"
    assert paths.fastq_dir == tmp_path / "base" / "fastq"
    assert paths.split_fastq_dir == tmp_path / "base" / "split_fastq"
    assert paths.barcode_dir == tmp_path / "base" / "barcodes"
    assert paths.star_dir == tmp_path / "base" / "star"

    assert paths.csv_log_dir == tmp_path / "base" / "logs" / "csv_logs"
    assert paths.python_log_dir == tmp_path / "base" / "logs" / "python_logs"
    assert paths.split_log_dir == tmp_path / "base" / "logs" / "split_logs"

    assert paths.star_genome_dir == tmp_path / "base" / "star" / "genome"
    assert paths.star_output_dir == tmp_path / "base" / "star" / "star_out"


def test_config_paths_get_all_directories(dummy_config, tmp_path):
    paths = ConfigPaths(config=dummy_config, base_dir=tmp_path)
    all_dirs = paths.get_all_directories()

    expected = [
        paths.sra_lists_dir,
        paths.sra_output_dir,
        paths.fastq_dir,
        paths.split_fastq_dir,
        paths.barcode_dir,
        paths.star_genome_dir,
        paths.star_output_dir,
        paths.csv_log_dir,
        paths.python_log_dir,
        paths.split_log_dir,
    ]

    assert all_dirs == expected