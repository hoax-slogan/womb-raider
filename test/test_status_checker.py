from pathlib import Path
from ..status_checker import DownloadStatusChecker


def test_file_exists(tmp_path):
    accession = "SRR000001"
    output_dir = tmp_path / accession
    output_dir.mkdir(parents=True)
    file_path = output_dir / f"{accession}.sra"
    file_path.touch()

    checker = DownloadStatusChecker(tmp_path)
    assert checker.check_status(accession) == "Already Exists"
    assert checker.confirm_download(accession) == "Download OK!"


def test_file_missing(tmp_path):
    checker = DownloadStatusChecker(tmp_path)
    accession = "SRR999999"
    assert checker.check_status(accession) is None
    assert checker.confirm_download(accession) == "Download Failed"