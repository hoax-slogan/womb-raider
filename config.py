import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

from .log_manager import LogManager
from .log_setup import setup_logging


load_dotenv()


class Config:
    def __init__(self, config_file="config.yaml", base_dir=None, *,
                safe: bool = True, setup_logs: bool = True):

        self.script_dir = Path(__file__).resolve().parent
        self.base_dir = base_dir or self.script_dir.parent

        self._load_config(config_file)
        self._set_paths()
        self._set_database_url()

        if safe:
            self._ensure_directories_exist()
        
        if setup_logs:
            self._initialize_logging()


    def _set_database_url(self):
        self.DATABASE_URL = os.getenv("DATABASE_URL")
        if not self.DATABASE_URL:
            raise ValueError("Missing DATABASE_URL in environment. Please check your .env file.")


    def _load_config(self, config_file):
        with open(self.script_dir / config_file, "r") as f:
            self.config = yaml.safe_load(f)


    def _path(self, *args):
        return self.base_dir.joinpath(*args)


    def _set_paths(self):
        self.DATA_DIR = self._path(self.config["data_dir"])

        subdirs = self.config["subdirs"]
        self.SRA_LISTS_DIR = self.DATA_DIR / subdirs["lists"]
        self.SRA_OUTPUT_DIR = self.DATA_DIR / subdirs["output"]
        self.LOGS_DIR = self.DATA_DIR / subdirs["logs"]
        self.FASTQ_DIR = self.DATA_DIR / subdirs["fastq"]
        self.STAR_DIR = self.DATA_DIR / subdirs["star"]

        logs = self.config["logs"]
        self.CSV_LOG_DIR = self.LOGS_DIR / logs["csv"]
        self.PYTHON_LOG_DIR = self.LOGS_DIR / logs["python"]

        star = self.config["star"]
        self.STAR_GENOME_DIR = self.STAR_DIR / star["genome_dir"]
        self.STAR_OUTPUT_DIR = self.STAR_DIR / star["star_output"]


    def _ensure_directories_exist(self):
        dirs = [
            self.SRA_LISTS_DIR,
            self.SRA_OUTPUT_DIR,
            self.FASTQ_DIR,
            self.STAR_GENOME_DIR,
            self.STAR_OUTPUT_DIR,
            self.CSV_LOG_DIR,
            self.PYTHON_LOG_DIR,
        ]
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)


    def _initialize_logging(self):
        log_manager = LogManager(self.CSV_LOG_DIR, self.PYTHON_LOG_DIR)
        python_log_path = log_manager.generate_python_log()
        setup_logging(python_log_path)