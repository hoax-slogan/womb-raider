import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from ..star_runner import STARRunner


@pytest.fixture
def mock_star_runner():
    return STARRunner(
        genome_dir=Path("/fake/genome_dir"),
        output_dir=Path("/fake/output_dir"),
        output_prefix="STAR_",
        threads=4
    )


@patch("pipeline.star_runner.subprocess.run")
def test_star_align_success(mock_run, mock_star_runner):
    # Simulate successful STAR execution
    mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

    fastq_files = [Path("fake_R1.fastq"), Path("fake_R2.fastq")]
    result = mock_star_runner.align("FAKE_ACC", fastq_files)

    # Check command call
    cmd = mock_star_runner._build_star_command(fastq_files)
    mock_run.assert_called_once_with(cmd, capture_output=True, text=True)

    # Output SAM file path
    assert result.name == "STAR_Aligned.out.sam"


@patch("pipeline.star_runner.subprocess.run")
def test_star_align_failure(mock_run, mock_star_runner):
    # Simulate failure in STAR
    mock_run.return_value = MagicMock(returncode=1, stderr="Some STAR error")

    fastq_files = [Path("fake_R1.fastq"), Path("fake_R2.fastq")]

    with pytest.raises(RuntimeError, match="STAR alignment failed"):
        mock_star_runner.align("FAIL_ACC", fastq_files)


def test_star_align_invalid_fastq(mock_star_runner):
    # Only one FASTQ file — should raise ValueError
    with pytest.raises(ValueError, match="paired-end FASTQ"):
        mock_star_runner.align("ACC", [Path("only_one.fastq")])