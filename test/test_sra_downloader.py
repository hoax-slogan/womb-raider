import pytest
import subprocess
from unittest.mock import MagicMock, patch
from ..sra_downloader import SRADownloader


def test_download_skips_if_exists(tmp_path):
    mock_log = MagicMock()
    mock_validator = MagicMock()
    mock_status = MagicMock()

    # Simulate file already exists
    mock_status.check_status.return_value = "Already Exists"
    mock_validator.validate.return_value = "Valid"

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log,
        validator=mock_validator,
        status_checker=mock_status
    )

    result = downloader.download("SRR123456", "source.txt")
    
    assert result == ["SRR123456", "Already Exists", "Valid", "source.txt"]
    mock_validator.validate.assert_called_once_with("SRR123456")
    mock_status.confirm_download.assert_not_called()


# mocks subprocess.run, assumes no file is present initially
# and ensures everything downstream still happens
@patch("pipeline.sra_downloader.subprocess.run")
def test_download_success_after_run(mock_subprocess, tmp_path):
    mock_log = MagicMock()
    mock_validator = MagicMock()
    mock_status = MagicMock()

    # Simulate file not present, so download is needed
    mock_status.check_status.return_value = None
    mock_status.confirm_download.return_value = "Download OK!"
    mock_validator.validate.return_value = "Valid"

    mock_subprocess.return_value = None  # simulate success

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log,
        validator=mock_validator,
        status_checker=mock_status,
    )

    result = downloader.download("SRR999999", "source.txt")

    assert result == ["SRR999999", "Download OK!", "Valid", "source.txt"]
    mock_subprocess.assert_called_once()
    mock_status.confirm_download.assert_called_once_with("SRR999999")
    mock_validator.validate.assert_called_once_with("SRR999999")


# test retry logic by failing once, then succeeding
@patch("pipeline.sra_downloader.subprocess.run")
@patch("pipeline.sra_downloader.time.sleep", return_value=None)
def test_download_retries_then_succeeds(mock_sleep, mock_subprocess, tmp_path):
    mock_log = MagicMock()
    mock_validator = MagicMock()
    mock_status = MagicMock()

    mock_status.check_status.return_value = None
    mock_status.confirm_download.return_value = "Download OK!"
    mock_validator.validate.return_value = "Valid"

    # First call fails, second succeeds
    mock_subprocess.side_effect = [
        subprocess.CalledProcessError(1, "cmd"),
        None,
    ]

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log,
        validator=mock_validator,
        status_checker=mock_status,
        max_retries=2,
    )

    result = downloader.download("SRR000111", "sourcefile.txt")

    assert result == ["SRR000111", "Download OK!", "Valid", "sourcefile.txt"]
    assert mock_subprocess.call_count == 2
    assert mock_sleep.call_count == 1


# ensures if all retries fail, still recieve a result 
@patch("pipeline.sra_downloader.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd"))
@patch("pipeline.sra_downloader.time.sleep", return_value=None)
def test_download_all_retries_fail(mock_sleep, mock_subprocess, tmp_path):
    mock_log = MagicMock()
    mock_validator = MagicMock()
    mock_status = MagicMock()

    mock_status.check_status.return_value = None
    mock_status.confirm_download.return_value = "Download Failed"
    mock_validator.validate.return_value = "Invalid"

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log,
        validator=mock_validator,
        status_checker=mock_status,
        max_retries=3,
    )

    result = downloader.download("SRRFAIL", "failfile.txt")

    assert result == ["SRRFAIL", "Download Failed", "Invalid", "failfile.txt"]
    assert mock_subprocess.call_count == 3
    assert mock_sleep.call_count == 3


@patch("pipeline.sra_downloader.get_sra_lists")
@patch("pipeline.sra_downloader.Pool")
@patch("pipeline.sra_downloader.tqdm", side_effect=lambda x, total=None: x)
def test_process_sra_lists(mock_tqdm, mock_pool_class, mock_get_sra_lists, tmp_path):
    mock_log_manager = MagicMock()
    mock_validator = MagicMock()
    mock_status_checker = MagicMock()

    fake_file = tmp_path / "fake_list.txt"
    fake_file.write_text("SRR000001\nSRR000002\n")
    mock_get_sra_lists.return_value = [fake_file]
    mock_log_manager.load_accessions_from_file.return_value = ["SRR000001", "SRR000002"]

    # Simulated return from pool.imap()
    expected_results = [["A", "OK", "Valid", "src"], ["B", "Fail", "Invalid", "src"]]
    mock_pool = MagicMock()
    mock_pool.__enter__.return_value.imap.return_value = expected_results
    mock_pool_class.return_value = mock_pool

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log_manager,
        validator=mock_validator,
        status_checker=mock_status_checker,
    )

    downloader.process_sra_lists()

    mock_get_sra_lists.assert_called_once_with(tmp_path)
    mock_log_manager.load_accessions_from_file.assert_called_once_with(fake_file)
    mock_log_manager.write_csv_log.assert_called_once_with(expected_results, tmp_path / "log.csv")


def test_retry_failed_no_accessions(tmp_path):
    mock_log_manager = MagicMock()
    mock_log_manager.get_failed_accessions.return_value = []

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log_manager,
        validator=MagicMock(),
        status_checker=MagicMock(),
    )

    downloader.retry_failed()

    mock_log_manager.get_failed_accessions.assert_called_once_with(tmp_path / "log.csv")
    mock_log_manager.write_csv_log.assert_not_called()


@patch("pipeline.sra_downloader.Pool")
@patch("pipeline.sra_downloader.tqdm", side_effect=lambda x, total=None: x)
def test_retry_failed_behavioral(mock_tqdm, mock_pool_class, tmp_path):
    mock_log_manager = MagicMock()
    mock_validator = MagicMock()
    mock_status_checker = MagicMock()

    mock_log_manager.get_failed_accessions.return_value = ["SRR111", "SRR222"]
    expected_results = [
        ["SRR111", "Download OK!", "Valid", "retry"],
        ["SRR222", "Download OK!", "Valid", "retry"]
    ]

    mock_pool = MagicMock()
    mock_pool.__enter__.return_value.imap.return_value = expected_results
    mock_pool_class.return_value = mock_pool

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log_manager,
        validator=mock_validator,
        status_checker=mock_status_checker,
    )

    downloader.retry_failed()

    mock_log_manager.get_failed_accessions.assert_called_once_with(tmp_path / "log.csv")
    mock_log_manager.write_csv_log.assert_called_once_with(expected_results, tmp_path / "log.csv")


@patch("pipeline.sra_downloader.Pool")
@patch("pipeline.sra_downloader.tqdm", side_effect=lambda x, total=None: x)
@patch("pipeline.sra_downloader.SRADownloader.download", side_effect=lambda acc, src: [acc, "Download OK!", "Valid", src])
def test_retry_failed_structural(mock_download, mock_tqdm, mock_pool_class, tmp_path):
    mock_log_manager = MagicMock()
    mock_validator = MagicMock()
    mock_status_checker = MagicMock()

    mock_log_manager.get_failed_accessions.return_value = ["SRR111", "SRR222"]

    # Don't short-circuit pool.imap, instead let the lambda run
    mock_pool = MagicMock()
    mock_pool.__enter__.return_value.imap.side_effect = lambda f, items: [f(item) for item in items]
    mock_pool_class.return_value = mock_pool

    downloader = SRADownloader(
        output_dir=tmp_path,
        sra_lists_dir=tmp_path,
        csv_log_path=tmp_path / "log.csv",
        log_manager=mock_log_manager,
        validator=mock_validator,
        status_checker=mock_status_checker,
    )

    downloader.retry_failed()

    mock_download.assert_any_call("SRR111", "retry")
    mock_download.assert_any_call("SRR222", "retry")
    assert mock_download.call_count == 2