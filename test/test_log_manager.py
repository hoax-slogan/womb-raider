from pathlib import Path
from ..log_manager import LogManager


def test_generate_csv_log_creates_file_with_header(tmp_path):
    log_manager = LogManager(csv_log_dir=tmp_path, python_log_dir=tmp_path)
    log_path = log_manager.generate_csv_log()

    assert log_path.exists()

    with log_path.open("r") as f:
        lines = f.readlines()

    assert lines[0].strip() == "Accession,Download Status,Validation Status,Source File"
    assert len(lines) == 1  


def test_write_csv_log_appends_entries(tmp_path):
    log_manager = LogManager(csv_log_dir=tmp_path, python_log_dir=tmp_path)
    log_path = tmp_path / "test_log.csv"

    # Create the file with a header
    log_path.write_text("Accession,Download Status,Validation Status,Source File\n")

    entries = [
        ["SRR000001", "Download OK!", "Valid", "source1.txt"],
        ["SRR000002", "Download Failed", "Invalid", "source2.txt"]
    ]

    log_manager.write_csv_log(entries, log_path)

    with log_path.open("r") as f:
        lines = [line.strip() for line in f.readlines()]

    assert len(lines) == 3  # 1 header + 2 entries
    assert lines[1] == "SRR000001,Download OK!,Valid,source1.txt"
    assert lines[2] == "SRR000002,Download Failed,Invalid,source2.txt"


def test_load_accessions_from_file(tmp_path):
    file = tmp_path / "accessions.txt"
    file.write_text("SRR000001\nSRR000002\n\n")
    log_manager = LogManager(csv_log_dir=tmp_path, python_log_dir=tmp_path)
    accessions = log_manager.load_accessions_from_file(file)
    assert accessions == ["SRR000001", "SRR000002"]


def test_get_failed_accessions(tmp_path):
    log_path = tmp_path / "log.csv"
    log_path.write_text(
        "Accession,Download Status,Validation Status,Source File\n"
        "SRR000001,Download OK!,Valid,source1.txt\n"
        "SRR000002,Download Failed,Valid,source2.txt\n"
        "SRR000003,Download OK!,Invalid,someerror\n"
        "SRR000004,Download Failed,Invalid,someerror\n"
    )

    log_manager = LogManager(csv_log_dir=tmp_path, python_log_dir=tmp_path)
    failed = log_manager.get_failed_accessions(log_path)

    assert failed == ["SRR000002", "SRR000003", "SRR000004"]


def test_get_failed_accessions_missing_log_file(tmp_path, caplog):
    log_path = tmp_path / "missing_log.csv"
    log_manager = LogManager(csv_log_dir=tmp_path, python_log_dir=tmp_path)

    with caplog.at_level("WARNING"):
        failed = log_manager.get_failed_accessions(log_path)

    assert failed == []
    assert f"Log file does not exist: {log_path}" in caplog.text