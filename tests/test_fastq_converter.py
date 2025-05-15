import subprocess
from unittest.mock import patch, MagicMock

import pytest

from ..fastq_converter import FASTQConverter
from ..enums import ConversionStatus
from ..tool_results import FASTQConversionResult


@pytest.fixture
def converter(tmp_path):
    return FASTQConverter(output_dir=tmp_path, threads=4)


def test_build_command(converter):
    cmd = converter._build_fasterq_command("SRR123")
    assert cmd == [
        "fasterq-dump", "SRR123",
        "--outdir", str(converter.output_dir),
        "--threads", "4"
    ]


def test_get_fastq_paths(converter):
    paths = converter.get_fastq_paths("SRR123")
    assert len(paths) == 2
    assert paths[0].name == "SRR123_1.fastq"
    assert paths[1].name == "SRR123_2.fastq"
    assert paths[0].parent == converter.output_dir


@patch("pipeline.fastq_converter.subprocess.run")
def test_convert_success(mock_run, converter):
    mock_run.return_value = MagicMock(returncode=0)
    result = converter.convert("SRR123")

    assert isinstance(result, FASTQConversionResult)
    assert result.status == ConversionStatus.SUCCESS
    assert len(result.output_files) == 2
    assert result.output_files[0].name.endswith("_1.fastq")


@patch("pipeline.fastq_converter.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd", stderr="simulated error"))
def test_convert_failure(mock_run, converter):
    result = converter.convert("SRR123")

    assert result.status == ConversionStatus.FAILED
    assert result.output_files == []
    assert "simulated error" in result.error_message