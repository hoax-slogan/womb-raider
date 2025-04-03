import pytest
from unittest.mock import MagicMock
from pathlib import Path

from ..job_runner import JobRunner


@pytest.fixture
def job_runner():
    return JobRunner(
        output_dir=Path("/fake/output"),
        manifest_manager=MagicMock(),
        validator=MagicMock(),
        status_checker=MagicMock(),
        s3_handler=None,
        fastq_converter=None,
        cleanup_local=True,
        logger=MagicMock()
    )


def test_cleanup_files_removes_files(job_runner):
    mock_fastq1 = MagicMock()
    mock_fastq2 = MagicMock()
    mock_sra = MagicMock()

    mock_fastq1.exists.return_value = True
    mock_fastq2.exists.return_value = True
    mock_sra.exists.return_value = True

    accession = "SRR123456"

    # Patch output_dir / accession / file path
    job_runner.output_dir = MagicMock()
    mock_accession_dir = MagicMock()
    job_runner.output_dir.__truediv__.return_value = mock_accession_dir
    mock_accession_dir.__truediv__.return_value = mock_sra

    job_runner._cleanup_files(accession, [mock_fastq1, mock_fastq2])

    mock_fastq1.unlink.assert_called_once()
    mock_fastq2.unlink.assert_called_once()
    mock_sra.unlink.assert_called_once()


def test_cleanup_directory_skips_non_empty(job_runner):
    accession = "SRR123456"
    mock_accession_dir = MagicMock()
    mock_accession_dir.exists.return_value = True
    mock_accession_dir.iterdir.return_value = iter(["something"])

    job_runner.output_dir = MagicMock()
    job_runner.output_dir.__truediv__.return_value = mock_accession_dir

    job_runner._cleanup_directory(accession)
    mock_accession_dir.rmdir.assert_not_called()


def test_cleanup_directory_removes_empty(job_runner):
    accession = "SRR123456"
    mock_accession_dir = MagicMock()
    mock_accession_dir.exists.return_value = True
    mock_accession_dir.iterdir.return_value = iter([])

    job_runner.output_dir = MagicMock()
    job_runner.output_dir.__truediv__.return_value = mock_accession_dir

    job_runner._cleanup_directory(accession)
    mock_accession_dir.rmdir.assert_called_once()