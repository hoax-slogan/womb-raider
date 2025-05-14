import pytest

from ...logs.manager import LogManager
from ...config.path_structs import LogPaths
from ...enums import StepStatus
from ...constants import CSV_HEADER


@pytest.fixture
def log_paths(tmp_path) -> LogPaths:
    return LogPaths(
        csv_log_dir=tmp_path / "csv",
        python_log_dir=tmp_path / "python",
        split_log_dir=tmp_path / "split"
    )


@pytest.fixture
def log_manager(log_paths) -> LogManager:
    for path in [log_paths.csv_log_dir, log_paths.python_log_dir, log_paths.split_log_dir]:
        path.mkdir(parents=True, exist_ok=True)
    return LogManager(paths=log_paths)


def test_generate_csv_log_creates_file_with_header(log_manager: LogManager):
    log_path = log_manager.generate_csv_log()
    assert log_path.exists()
    with log_path.open("r") as f:
        lines = f.readlines()
    assert lines[0].strip() == ",".join(CSV_HEADER)
    assert len(lines) == 1


def test_generate_python_log_creates_file(log_manager: LogManager):
    log_path = log_manager.generate_python_log()
    assert log_path.parent.name == "python"
    assert log_path.name.startswith("pipeline_")
    assert log_path.suffix == ".log"


def test_write_csv_log_appends_entries(log_manager: LogManager):
    log_path = log_manager._logs.csv_log_dir / "test.csv"
    log_path.write_text(",".join(CSV_HEADER) + "\n")
    entries = [
        ["SRR000001", "Download OK!", "Valid", "source1.txt"],
        ["SRR000002", "Download Failed", "Invalid", "source2.txt"]
    ]
    log_manager.write_csv_log(entries, log_path)
    with log_path.open("r") as f:
        lines = [line.strip() for line in f.readlines()]
    assert len(lines) == 3
    assert lines[1] == "SRR000001,Download OK!,Valid,source1.txt"
    assert lines[2] == "SRR000002,Download Failed,Invalid,source2.txt"


def test_load_accessions_from_file(log_manager: LogManager):
    file = log_manager._logs.csv_log_dir / "accessions.txt"
    file.write_text("SRR000001\nSRR000002\n\n")
    accessions = log_manager.load_accessions_from_file(file)
    assert accessions == ["SRR000001", "SRR000002"]


def test_get_failed_accessions(log_manager: LogManager):
    log_path = log_manager._logs.csv_log_dir / "log.csv"
    log_path.write_text(
        "Accession,Step1,Step2,Step3,Step4,Step5\n"
        f"SRR000001,{StepStatus.SUCCESS.value},{StepStatus.SUCCESS.value},x,x,x\n"
        f"SRR000002,{StepStatus.FAILED.value},{StepStatus.SUCCESS.value},x,x,x\n"
        f"SRR000003,{StepStatus.SUCCESS.value},{StepStatus.FAILED.value},x,x,x\n"
        f"SRR000004,{StepStatus.FAILED.value},{StepStatus.FAILED.value},x,x,x\n"
    )
    failed = log_manager.get_failed_accessions(log_path)
    assert failed == ["SRR000002", "SRR000003", "SRR000004"]


def test_get_failed_accessions_missing_log_file(log_manager: LogManager, caplog):
    log_path = log_manager._logs.csv_log_dir / "missing_log.csv"
    with caplog.at_level("WARNING"):
        failed = log_manager.get_failed_accessions(log_path)
    assert failed == []
    assert f"Log file does not exist: {log_path}" in caplog.text