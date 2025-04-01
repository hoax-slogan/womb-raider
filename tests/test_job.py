import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import subprocess
from ..job import Job, StepStatus, PipelineStep


@pytest.fixture
def mock_manifest_manager():
    # Mock job status object with default step statuses
    mock_status = MagicMock()
    mock_status.download = None
    mock_status.validate = None
    mock_status.convert = None
    mock_status.upload = None

    mock_mm = MagicMock()
    mock_mm.get_or_create_job.return_value = mock_status
    return mock_mm, mock_status


@pytest.fixture
def fake_job(mock_manifest_manager):
    mm, status = mock_manifest_manager
    job = Job(
        accession="SRR_FAKE123",
        source_file="fake_list.txt",
        output_dir=Path("/fake/output"),
        validator=MagicMock(),
        status_checker=MagicMock(),
        manifest_manager=mm,
        fastq_converter=MagicMock(),
        s3_handler=MagicMock(),
    )
    return job, status


@patch("pipeline.job.subprocess.run")
def test_run_download_success(mock_run, fake_job):
    job, status = fake_job
    job.status_checker.check_status.return_value = "Not Found"
    job.status_checker.confirm_download.return_value = "Download OK!"

    result = job.run_download()

    assert result is True
    assert status.download == StepStatus.SUCCESS
    mock_run.assert_called_once()


@patch("pipeline.job.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd"))
def test_run_download_failure(mock_run, fake_job):
    job, status = fake_job
    job.status_checker.check_status.return_value = "Not Found"

    result = job.run_download()

    assert result is False
    assert status.download == StepStatus.FAILED
    mock_run.assert_called_once()


def test_run_download_skipped(fake_job):
    job, status = fake_job
    job.status_checker.check_status.return_value = "Already Exists"

    result = job.run_download()

    assert result is True
    assert status.download == StepStatus.SKIPPED


def test_run_validation_success(fake_job):
    job, status = fake_job
    job.validator.validate.return_value = "Valid"

    result = job.run_validation()

    assert result == "Valid"
    assert status.validate == StepStatus.SUCCESS


def test_run_validation_failure(fake_job):
    job, status = fake_job
    job.validator.validate.return_value = "Invalid: something bad"

    result = job.run_validation()

    assert "Invalid" in result
    assert status.validate == StepStatus.FAILED


def test_run_conversion_success(fake_job):
    job, status = fake_job
    job.fastq_converter.convert.return_value = True

    job.run_conversion()

    assert status.convert == StepStatus.SUCCESS


def test_run_conversion_failure(fake_job):
    job, status = fake_job
    job.fastq_converter.convert.return_value = False

    job.run_conversion()

    assert status.convert == StepStatus.FAILED


def test_run_upload_success(fake_job):
    job, status = fake_job
    path = Path("somefile.txt")

    job.run_upload(path)

    assert status.upload == StepStatus.SUCCESS
    job.s3_handler.upload_file.assert_called_once_with(path)


def test_run_upload_failure(fake_job):
    job, status = fake_job
    path = Path("somefile.txt")
    job.s3_handler.upload_file.side_effect = Exception("lol kaboom")

    job.run_upload(path)

    assert status.upload == StepStatus.FAILED