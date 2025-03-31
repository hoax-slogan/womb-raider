import os
from pathlib import Path

import yaml
from dotenv import load_dotenv


load_dotenv()


class Config:
    def __init__(self, config_file="config.yaml"):
        self.script_dir = Path(__file__).resolve().parent
        self.base_dir = self.script_dir.parent

        self._load_config(config_file)
        self._set_paths()
        self._set_database_url()
    
    def _set_database_url(self):
        self.DATABASE_URL = os.getenv("DATABASE_URL")
        if not self.DATABASE_URL:
            raise ValueError("Missing DATABASE_URL in environment. Please check your .env file.")

    def _load_config(self, config_file):
        with open(self.script_dir / config_file, "r") as f:
            self.config = yaml.safe_load(f)

    def _path(self, *args):
        """Helper to build paths more cleanly."""
        return self.base_dir.joinpath(*args)

    def _set_paths(self):
        self.DATA_DIR = self._path(self.config["data_dir"])

        subdirs = self.config["subdirs"]
        self.SRA_LISTS_DIR = self.DATA_DIR / subdirs["lists"]
        self.SRA_OUTPUT_DIR = self.DATA_DIR / subdirs["output"]
        self.LOGS_DIR = self.DATA_DIR / subdirs["logs"]
        self.FASTQ_DIR = self.DATA_DIR / subdirs["fastq"]

        logs = self.config["logs"]
        self.CSV_LOG_DIR = self.LOGS_DIR / logs["csv"]
        self.PYTHON_LOG_DIR = self.LOGS_DIR / logs["python"]

    def ensure_directories_exist(self):
        dirs: list[Path] = [
            self.SRA_LISTS_DIR,
            self.SRA_OUTPUT_DIR,
            self.FASTQ_DIR,
            self.CSV_LOG_DIR,
            self.PYTHON_LOG_DIR,
        ]
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)
