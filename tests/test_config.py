import yaml
import pytest
from pathlib import Path
from ..config import Config


# setup the yaml file
@pytest.fixture
def mock_config_yaml(tmp_path):
    config_data = {
        "data_dir": "sra_data",
        "subdirs": {
            "lists": "sra_lists",
            "output": "sra_files",
            "logs": "logs",
            "fastq": "fastq_files",
            "star_output": "star_output"
        },
        "logs": {
            "csv": "csv_logs",
            "python": "python_logs"
        },
        "star": {
            "genome_dir": "genome_ref",
            "threads": 4,
            "output_prefix": "STAR_"
        }
    }

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    return config_file


def test_config_paths(mock_config_yaml):
    # Patch __file__ context by pretending the script is in tmp_path
    class DummyConfig(Config):
        def __init__(self, config_file):
            self.script_dir = mock_config_yaml.parent
            self.base_dir = self.script_dir.parent
            self._load_config(Path(config_file).name)
            self._set_paths()

    cfg = DummyConfig(config_file=mock_config_yaml)

    assert cfg.DATA_DIR.name == "sra_data"
    assert cfg.SRA_LISTS_DIR.name == "sra_lists"
    assert cfg.SRA_OUTPUT_DIR.name == "sra_files"
    assert cfg.LOGS_DIR.name == "logs"
    assert cfg.FASTQ_DIR.name == "fastq_files"
    assert cfg.STAR_OUTPUT_DIR.name == "star_output"
    assert cfg.CSV_LOG_DIR.name == "csv_logs"
    assert cfg.PYTHON_LOG_DIR.name == "python_logs"
    assert cfg.STAR_GENOME_DIR.name == "genome_ref"
    assert cfg.STAR_THREADS == 4
    assert cfg.STAR_OUTPUT_PREFIX == "STAR_"
    


def test_directory_creation(tmp_path, mock_config_yaml):
    class DummyConfig(Config):
        def __init__(self, config_file):
            self.script_dir = mock_config_yaml.parent
            self.base_dir = self.script_dir.parent
            self._load_config(Path(config_file).name)
            self._set_paths()

    cfg = DummyConfig(config_file=mock_config_yaml)
    cfg.ensure_directories_exist()

    # Check that the paths now exist
    assert cfg.SRA_LISTS_DIR.exists()
    assert cfg.SRA_OUTPUT_DIR.exists()
    assert cfg.FASTQ_DIR.exists()
    assert cfg.CSV_LOG_DIR.exists()
    assert cfg.PYTHON_LOG_DIR.exists()
    assert cfg.STAR_OUTPUT_DIR.exists()
