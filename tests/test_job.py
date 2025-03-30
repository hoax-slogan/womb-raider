import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import subprocess
from ..job import Job, StepStatus


@pytest.fixture
def fake_job():
    return Job(
        accession="SRR_FAKE123",
        source_file="fake_list.txt",
        output_dir=Path("/fake/output"),
        validator=MagicMock(),
        status_checker=MagicMock(),
        fastq_converter=MagicMock(),
        s3_handler=MagicMock(),
    )


@patch("pipeline.job.subprocess.run")
def test_run_download_success(mock_run, fake_job):
    fake_job.status_checker.check_status.return_value = "Not Found"
    fake_job.status_checker.confirm_download.return_value = "Download OK!"
    result = fake_job.run_download()

    assert result is True
    assert fake_job.status.download == StepStatus.SUCCESS
    mock_run.assert_called_once()


@patch("pipeline.job.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd"))
def test_run_download_failure(mock_run, fake_job):
    fake_job.status_checker.check_status.return_value = "Not Found"
    result = fake_job.run_download()

    assert result is False
    assert fake_job.status.download == StepStatus.FAILED
    mock_run.assert_called_once()


def test_run_download_skipped(fake_job):
    fake_job.status_checker.check_status.return_value = "Already Exists"
    result = fake_job.run_download()

    assert result is True
    assert fake_job.status.download == StepStatus.SKIPPED


def test_run_validation_success(fake_job):
    fake_job.validator.validate.return_value = "Valid"
    result = fake_job.run_validation()

    assert result == "Valid"
    assert fake_job.status.validate == StepStatus.SUCCESS


def test_run_validation_failure(fake_job):
    fake_job.validator.validate.return_value = "Invalid: corrupt"
    result = fake_job.run_validation()

    assert "Invalid" in result
    assert fake_job.status.validate == StepStatus.FAILED


def test_run_conversion_success(fake_job):
    fake_job.fastq_converter.convert.return_value = True
    fake_job.run_conversion()

    assert fake_job.status.convert == StepStatus.SUCCESS


def test_run_conversion_failure(fake_job):
    fake_job.fastq_converter.convert.return_value = False
    fake_job.run_conversion()

    assert fake_job.status.convert == StepStatus.FAILED


def test_run_upload_success(fake_job):
    local_path = Path("somefile.txt")
    fake_job.run_upload(local_path)

    assert fake_job.status.upload == StepStatus.SUCCESS
    fake_job.s3_handler.upload_file.assert_called_once_with(local_path)


def test_run_upload_failure(fake_job):
    local_path = Path("somefile.txt")
    fake_job.s3_handler.upload_file.side_effect = Exception("S3 exploded")
    fake_job.run_upload(local_path)
    
    assert fake_job.status.upload == StepStatus.FAILED