import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import subprocess

from ..job import Job
from ..enums import StepStatus


class DummyStatus:
    def __init__(self):
        self.download_status = None
        self.validate_status = None
        self.convert_status = None
        self.align_status = None
        self.upload_status = None
        self.pipeline_status = None


@pytest.fixture
def mock_manifest_manager():
    dummy_status = DummyStatus()
    mock_mm = MagicMock()
    mock_mm.get_or_create_job.return_value = dummy_status
    return mock_mm, dummy_status


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
        star_runner=MagicMock()
    )
    return job, status


@patch("pipeline.job.subprocess.run")
def test_run_download_success(mock_run, fake_job):
    job, status = fake_job
    job.status_checker.check_status.return_value = "Not Found"
    job.status_checker.confirm_download.return_value = "Download OK!"

    result = job.run_download()

    assert result is True
    assert status.download_status == StepStatus.SUCCESS
    mock_run.assert_called_once()


@patch("pipeline.job.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd", stderr="simulated error"))
def test_run_download_failure(mock_run, fake_job):
    job, status = fake_job
    job.status_checker.check_status.return_value = "Not Found"

    result = job.run_download()

    assert result is False
    assert status.download_status == StepStatus.FAILED
    mock_run.assert_called_once()


def test_run_download_skipped(fake_job):
    job, status = fake_job
    job.status_checker.check_status.return_value = "Already Exists"
    job.status_checker.confirm_download.return_value = "Download OK!"

    result = job.run_download()

    assert result is True
    assert status.download_status == StepStatus.SUCCESS


def test_run_validation_success(fake_job):
    job, status = fake_job
    job.validator.validate.return_value = "Valid"

    result = job.run_validation()

    assert result == "Valid"
    assert status.validate_status == StepStatus.SUCCESS


def test_run_validation_failure(fake_job):
    job, status = fake_job
    job.validator.validate.return_value = "Invalid: something bad"

    result = job.run_validation()

    assert "Invalid" in result
    assert status.validate_status == StepStatus.FAILED


@patch("pathlib.Path.exists", return_value=True)
def test_run_conversion_success(mock_exists, fake_job):
    job, status = fake_job
    job.fastq_converter.convert.return_value = True
    job.fastq_converter.output_dir = Path("/fake/fastq_dir")

    output = job.run_conversion()

    assert status.convert_status == StepStatus.SUCCESS
    assert len(output) == 2


def test_run_conversion_failure(fake_job):
    job, status = fake_job
    job.fastq_converter.convert.return_value = False

    job.run_conversion()

    assert status.convert_status == StepStatus.FAILED


def test_run_alignment_success(fake_job):
    job, status = fake_job
    job.fastq_converter.get_fastq_paths.return_value = [Path("r1.fastq"), Path("r2.fastq")]
    expected_outputs = [
        Path("STAR_Aligned.out.sam"),
        Path("SJ.out.tab"),
        Path("Log.out"),
        Path("Log.final.out")
    ]
    job.star_runner.align.return_value = expected_outputs

    result = job.run_alignment()

    assert result == expected_outputs
    assert status.align_status == StepStatus.SUCCESS


def test_run_alignment_failure(fake_job):
    job, status = fake_job
    job.fastq_converter.get_fastq_paths.return_value = [Path("r1.fastq"), Path("r2.fastq")]
    job.star_runner.align.side_effect = Exception("align boom")

    result = job.run_alignment()

    assert result == []
    assert status.align_status == StepStatus.FAILED


def test_run_upload_success(fake_job):
    job, status = fake_job
    path = Path("somefile.txt")

    job.run_upload(path)

    assert status.upload_status == StepStatus.SUCCESS
    job.s3_handler.upload_file.assert_called_once_with(path)


def test_run_upload_failure(fake_job):
    job, status = fake_job
    path = Path("somefile.txt")
    job.s3_handler.upload_file.side_effect = Exception("kaboom")

    job.run_upload(path)

    assert status.upload_status == StepStatus.FAILED