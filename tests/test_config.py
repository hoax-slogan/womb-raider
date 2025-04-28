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
            "star": "star"
        },
        "logs": {
            "csv": "csv_logs",
            "python": "python_logs"
        },
        "star": {
            "genome_dir": "genome_ref",
            "star_output": "star_output"
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

    assert cfg.data_dir.name == "sra_data"
    assert cfg.sra_lists_dir.name == "sra_lists"
    assert cfg.sra_output_dir.name == "sra_files"
    assert cfg.logs_dir.name == "logs"
    assert cfg.fastq_dir.name == "fastq_files"
    assert cfg.csv_log_dir.name == "csv_logs"
    assert cfg.python_log_dir.name == "python_logs"
    assert cfg.star_dir.name == "star"
    assert cfg.star_genome_dir.name == "genome_ref"
    assert cfg.star_output_dir.name == "star_output"


def test_directory_creation(tmp_path, mock_config_yaml):
    class DummyConfig(Config):
        def __init__(self, config_file):
            self.script_dir = mock_config_yaml.parent
            self.base_dir = self.script_dir.parent
            self._load_config(Path(config_file).name)
            self._set_paths()

    cfg = DummyConfig(config_file=mock_config_yaml)
    cfg._ensure_directories_exist()

    # Check that the paths now exist
    assert cfg.sra_lists_dir.exists()
    assert cfg.sra_output_dir.exists()
    assert cfg.fastq_dir.exists()
    assert cfg.star_dir.exists()
    assert cfg.csv_log_dir.exists()
    assert cfg.python_log_dir.exists()
    assert cfg.star_genome_dir.exists()
    assert cfg.star_output_dir.exists()
