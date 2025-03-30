import pytest
import subprocess
from unittest.mock import patch, MagicMock
from ..fastq_converter import FASTQConverter
from pathlib import Path


@patch("pipeline.fastq_converter.subprocess.run")
def test_fastq_convert_success(mock_run):
    mock_run.return_value = MagicMock(returncode=0)

    converter = FASTQConverter(output_dir=Path("/tmp"), threads=2)
    success = converter.convert("SRR123456")

    assert success is True
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert "fasterq-dump" in cmd
    assert "SRR123456" in cmd


@patch("pipeline.fastq_converter.subprocess.run")
def test_fastq_convert_failure(mock_run):
    mock_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd="fasterq-dump")

    converter = FASTQConverter(output_dir=Path("/tmp"), threads=2)
    success = converter.convert("SRR_FAIL_TEST")

    assert success is False
    mock_run.assert_called_once()