import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from types import SimpleNamespace
from pipeline.orchestrator import SRAOrchestrator
from pipeline.job import Job


@pytest.fixture
def orchestrator_setup():
    mock_log_manager = MagicMock()
    mock_manifest_manager = MagicMock()
    mock_log_manager.load_accessions_from_file.return_value = ["SRR123456", "SRR789012"]
    mock_log_manager.write_csv_log = MagicMock()
    mock_log_manager.get_failed_accessions.return_value = []

    orchestrator = SRAOrchestrator(
        output_dir=Path("/fake/output"),
        sra_lists_dir=Path("/fake/lists"),
        csv_log_path=Path("/fake/log.csv"),
        fastq_file_dir=Path("/fake/fastq"),
        log_manager=mock_log_manager,
        validator=MagicMock(),
        status_checker=MagicMock(),
        manifest_manager=mock_manifest_manager,
        convert_fastq=False,
        fastq_threads=4,
        max_retries=3,
        batch_size=2,
        s3_handler=None
    )

    return orchestrator, mock_log_manager


@patch("pipeline.orchestrator.get_sra_lists")
@patch("pipeline.orchestrator.SRAOrchestrator.process_batch")
def test_process_sra_lists(mock_process_batch, mock_get_sra_lists, orchestrator_setup):
    orchestrator, mock_log_manager = orchestrator_setup

    fake_sra_file = Path("fake_list.txt")
    mock_get_sra_lists.return_value = [fake_sra_file]

    mock_process_batch.return_value = [
        ["SRR123456", "Success", "fake_list.txt"],
        ["SRR789012", "Success", "fake_list.txt"]
    ]

    orchestrator.process_sra_lists()

    mock_log_manager.load_accessions_from_file.assert_called_once_with(fake_sra_file)
    mock_process_batch.assert_called_once()
    mock_log_manager.write_csv_log.assert_called_once()


@patch("pipeline.orchestrator.SRAOrchestrator.process_batch")
def test_retry_failed_with_failures(mock_process_batch, orchestrator_setup):
    orchestrator, mock_log_manager = orchestrator_setup
    mock_log_manager.get_failed_accessions.return_value = ["SRR_FAIL123"]

    mock_process_batch.return_value = [["SRR_FAIL123", "Failed", "retry"]]

    orchestrator.retry_failed()

    mock_log_manager.write_csv_log.assert_called_once_with(
        [["SRR_FAIL123", "Failed", "retry"]],
        Path("/fake/log.csv")
    )


def test_retry_failed_with_no_failures(orchestrator_setup):
    orchestrator, mock_log_manager = orchestrator_setup
    mock_log_manager.get_failed_accessions.return_value = []

    result = orchestrator.retry_failed()
    assert result is None
    mock_log_manager.write_csv_log.assert_not_called()


def test_get_fastq_converter_enabled():
    mock_log = MagicMock()
    mock_manifest_manager = MagicMock()

    orchestrator = SRAOrchestrator(
        output_dir=Path("/x"),
        sra_lists_dir=Path("/x"),
        csv_log_path=Path("/x.csv"),
        fastq_file_dir=Path("/fastq"),
        log_manager=mock_log,
        validator=MagicMock(),
        status_checker=MagicMock(),
        manifest_manager=mock_manifest_manager,
        convert_fastq=True,
        s3_handler=None,
    )

    converter = orchestrator._get_fastq_converter()
    assert converter is not None
    assert hasattr(converter, "convert")


def test_get_fastq_converter_disabled(orchestrator_setup):
    orchestrator, _ = orchestrator_setup
    assert orchestrator._get_fastq_converter() is None


def test_execute_job_runs_steps(monkeypatch, orchestrator_setup):
    orchestrator, _ = orchestrator_setup

    # Mock a Job-like object
    class FakeJob:
        def __init__(self):
            self.accession = "SRR987654"
            self.status = SimpleNamespace(download=SimpleNamespace(value="Success"))
            self.source_file = "source_file.txt"

        def run_download(self): self.download_called = True
        def run_validation(self): self.validation_called = True
        def run_conversion(self): self.conversion_called = True

    monkeypatch.setattr(orchestrator, "create_job", lambda accession, source_file, manifest_manager: FakeJob())

    result = orchestrator.execute_job(("SRR987654", "source_file.txt"))
    assert result.accession == "SRR987654"
    assert result.status.download.value == "Success"
    assert result.source_file == "source_file.txt"