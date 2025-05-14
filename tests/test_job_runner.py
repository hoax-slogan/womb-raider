import pytest
from unittest.mock import MagicMock
from pathlib import Path

from ..jobs.runner import JobRunner


@pytest.fixture
def job_runner():
    return JobRunner(
        output_dir=Path("/fake/output"),
        session_maker=MagicMock(),  # FIXED HERE
        validator=MagicMock(),
        status_checker=MagicMock(),
        star_runner=MagicMock(),
        s3_handler=None,
        fastq_converter=None,
        logger=MagicMock()
    )

def test_cleanup_fastq_files_removes_files(job_runner):
    mock_fastq1 = MagicMock()
    mock_fastq2 = MagicMock()
    mock_fastq1.exists.return_value = True
    mock_fastq2.exists.return_value = True

    job_runner._safe_unlink = MagicMock()

    job_runner._cleanup_fastq_files([mock_fastq1, mock_fastq2])

    job_runner._safe_unlink.assert_any_call(mock_fastq1)
    job_runner._safe_unlink.assert_any_call(mock_fastq2)


def test_cleanup_directory_skips_non_empty(job_runner):
    accession = "SRR123456"
    mock_accession_dir = MagicMock()
    mock_accession_dir.exists.return_value = True
    mock_accession_dir.iterdir.return_value = iter(["something"])

    job_runner.output_dir = MagicMock()
    job_runner.output_dir.__truediv__.return_value = mock_accession_dir

    job_runner._cleanup_directories(accession)
    mock_accession_dir.rmdir.assert_not_called()


def test_cleanup_directory_removes_empty(job_runner):
    accession = "SRR123456"
    mock_accession_dir = MagicMock()
    mock_accession_dir.exists.return_value = True
    mock_accession_dir.iterdir.return_value = iter([])

    job_runner.output_dir = MagicMock()
    job_runner.output_dir.__truediv__.return_value = mock_accession_dir

    job_runner._cleanup_directories(accession)
    mock_accession_dir.rmdir.assert_called_once()