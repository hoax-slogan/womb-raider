import pytest
from pathlib import Path

from ...config.config_runtime import setup_runtime
from ...config.config_paths import ConfigPaths
from ...config.config_schema import *
from ...config.path_structs import LogPaths


@pytest.fixture
def dummy_config():
    return WombRaiderConfig(
        data_dir="base",
        subdirs=SubdirsConfig(
            lists="lists",
            output="output",
            logs="logs",
            fastq="fastq",
            split_fastq="split_fastq",
            barcodes="barcodes",
            star="star"
        ),
        logs=LogsConfig(
            csv="csv_logs",
            python="python_logs",
            split="split_logs"
        ),
        star=StarConfig(
            genome_dir="genome",
            star_output="star_out"
        )
    )


def test_setup_runtime_creates_directories(tmp_path, dummy_config):
    paths = ConfigPaths(dummy_config, tmp_path)
    setup_runtime(paths, safe=True, setup_logs=False)

    for path in paths.get_all_directories():
        assert path.exists()
        assert path.is_dir()


def test_setup_runtime_calls_logging(monkeypatch, tmp_path, dummy_config):
    paths = ConfigPaths(dummy_config, tmp_path)
    log_calls = {}

    class DummyLogManager:
        def __init__(self, paths: LogPaths):
            log_calls["init_args"] = paths

        def generate_python_log(self):
            log_calls["log_created"] = True
            return tmp_path / "dummy.log"

    def dummy_setup_logging(path: Path):
        log_calls["logging_set_up"] = path

    # Patch the actual symbols used inside config_runtime
    monkeypatch.setattr("pipeline.logs.log_manager.LogManager", DummyLogManager)
    monkeypatch.setattr("pipeline.logs.log_setup.setup_logging", dummy_setup_logging)

    setup_runtime(paths, safe=False, setup_logs=True)

    assert isinstance(log_calls["init_args"], LogPaths)
    assert log_calls["log_created"] is True
    assert log_calls["logging_set_up"] == tmp_path / "dummy.log"