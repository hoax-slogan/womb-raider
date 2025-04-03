import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from ..job_orchestrator import SRAOrchestrator


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
        s3_handler=None,
        cleanup_local=False
    )

    return orchestrator, mock_log_manager


@patch("pipeline.job_orchestrator.get_sra_lists")
@patch("pipeline.job_orchestrator.SRAOrchestrator.process_batch")
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


@patch("pipeline.job_orchestrator.SRAOrchestrator.process_batch")
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


def test_execute_job_calls_job_runner(monkeypatch, orchestrator_setup):
    orchestrator, _ = orchestrator_setup

    called_with = {}

    class FakeJobRunner:
        def __init__(self, **kwargs):
            called_with['init'] = kwargs
        def run(self, accession, source_file):
            called_with['args'] = (accession, source_file)
            return ["SRR123456", "Success", "Success", "fake_list.txt"]

    monkeypatch.setattr("pipeline.job_orchestrator.JobRunner", FakeJobRunner)

    result = orchestrator.execute_job(("SRR123456", "fake_list.txt"))

    assert result == ["SRR123456", "Success", "Success", "fake_list.txt"]
    assert called_with['args'] == ("SRR123456", "fake_list.txt")
