import os
import pytest
import yaml

from ...config.config_loader import _resolve_threads, load_config, get_database_url


# _resolve_threads

@pytest.mark.parametrize("mock_cores, raw_input, expected", [
    (8, None, 6),              # 75%
    (4, "50%", 2),
    (16, "auto", 16),
    (2, "100%", 2),
    (12, 6, 6),
    (12, "6", 6),
])
def test_resolve_threads_valid(monkeypatch, mock_cores, raw_input, expected):
    monkeypatch.setattr(os, "cpu_count", lambda: mock_cores)
    assert _resolve_threads(raw_input) == expected


@pytest.mark.parametrize("invalid_input", ["notanumber", "105%%", [4], {"threads": 2}])
def test_resolve_threads_invalid(monkeypatch, invalid_input):
    monkeypatch.setattr(os, "cpu_count", lambda: 8)
    with pytest.raises(ValueError):
        _resolve_threads(invalid_input)


# load_config

@pytest.fixture
def sample_config_yaml():
    return {
        "data_dir": "some_dir",
        "subdirs": {
            "lists": "lists",
            "output": "output",
            "logs": "logs",
            "fastq": "fastq",
            "split_fastq": "split_fastq",
            "barcodes": "barcodes",
            "star": "star"
        },
        "logs": {
            "csv": "csv_logs",
            "python": "python_logs",
            "split": "split_logs"
        },
        "star": {
            "genome_dir": "genome",
            "star_output": "output"
        },
        "threads": "50%",
        "batch_size": 2,
        "max_retries": 10,
        "s3_bucket": "my-bucket",
        "s3_prefix": "prefix/"
    }


def test_load_config(tmp_path, sample_config_yaml, monkeypatch):
    monkeypatch.setattr(os, "cpu_count", lambda: 4)
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.safe_dump(sample_config_yaml, f)

    config = load_config(config_file)

    assert config.data_dir == "some_dir"
    assert config.subdirs.barcodes == "barcodes"
    assert config.logs.csv == "csv_logs"
    assert config.star.genome_dir == "genome"
    assert config.threads == 2  # 50% of 4 cores
    assert config.batch_size == 2
    assert config.s3_bucket == "my-bucket"
    assert config.s3_prefix == "prefix/"


# get_database_url

def test_get_database_url(monkeypatch):
    monkeypatch.setenv("database_url", "sqlite:///test.db")
    assert get_database_url() == "sqlite:///test.db"


def test_get_database_url_missing(monkeypatch):
    monkeypatch.delenv("database_url", raising=False)
    with pytest.raises(ValueError):
        get_database_url()