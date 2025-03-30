import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from ..orchestrator import SRAOrchestrator


@pytest.fixture
def orchestrator_setup():
    mock_log_manager = MagicMock()
    mock_log_manager.load_accessions_from_file.return_value = ["SRR123456", "SRR789012"]
    mock_log_manager.write_csv_log = MagicMock()

    orchestrator = SRAOrchestrator(
        output_dir=Path("/fake/output"),
        sra_lists_dir=Path("/fake/lists"),
        csv_log_path=Path("/fake/log.csv"),
        fastq_file_dir=Path("/fake/fastq"),
        log_manager=mock_log_manager,
        validator=MagicMock(),
        status_checker=MagicMock(),
        convert_fastq=False,
        fastq_threads=4,
        max_retries=3,
        batch_size=2,
        s3_handler=None
    )

    return orchestrator, mock_log_manager


@patch("pipeline.orchestrator.get_sra_lists")
@patch("pipeline.orchestrator.Pool")
def test_process_sra_lists(mock_pool_cls, mock_get_sra_lists, orchestrator_setup):
    orchestrator, mock_log_manager = orchestrator_setup

    # Fake SRA list file
    fake_sra_file = Path("fake_list.txt")
    mock_get_sra_lists.return_value = [fake_sra_file]

    # Fake multiprocessing pool
    mock_pool = MagicMock()
    mock_pool.__enter__.return_value = mock_pool  # For context manager
    mock_pool.imap.return_value = [
        ["SRR123456", "Success", "Valid", fake_sra_file],
        ["SRR789012", "Success", "Valid", fake_sra_file],
    ]
    mock_pool_cls.return_value = mock_pool

    orchestrator.process_sra_lists()

    # ✅ Ensure accessions were loaded
    mock_log_manager.load_accessions_from_file.assert_called_once_with(fake_sra_file)

    # ✅ Ensure imap was called with correct number of jobs
    assert mock_pool.imap.call_count == 1
    assert mock_log_manager.write_csv_log.call_count == 1