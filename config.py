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
        self._load_cli_settings()
        self._set_paths()
        self._set_database_url()

        if safe:
            self._ensure_directories_exist()
        
        if setup_logs:
            self._initialize_logging()


    def _set_database_url(self):
        self.database_url = os.getenv("database_url")
        if not self.database_url:
            raise ValueError("Missing database_url in environment. Please check your .env file.")


    def _load_config(self, config_file):
        with open(self.script_dir / config_file, "r") as f:
            self.config = yaml.safe_load(f)
    

    def _load_cli_settings(self):
        """Extract CLI-relevant settings after YAML is loaded."""
        self.batch_size = self.config.get("batch_size", 5)
        self.threads = self.config.get("threads", 4)
        self.max_retries = self.config.get("max_retries", 5)
        self.s3_bucket = self.config.get("s3_bucket", None)
        self.s3_prefix = self.config.get("s3_prefix", "")


    def _path(self, *args):
        return self.base_dir.joinpath(*args)


    def _set_paths(self):
        self.data_dir = self._path(self.config["data_dir"])

        subdirs = self.config["subdirs"]
        self.sra_lists_dir = self.data_dir / subdirs["lists"]
        self.sra_output_dir = self.data_dir / subdirs["output"]
        self.logs_dir = self.data_dir / subdirs["logs"]
        self.fastq_dir = self.data_dir / subdirs["fastq"]
        self.split_fastq_dir = self.data_dir / subdirs["split_fastq"]
        self.barcode_dir = self.data_dir / subdirs["barcodes"]
        self.star_dir = self.data_dir / subdirs["star"]

        logs = self.config["logs"]
        self.csv_log_dir = self.logs_dir / logs["csv"]
        self.python_log_dir = self.logs_dir / logs["python"]

        star = self.config["star"]
        self.star_genome_dir = self.star_dir / star["genome_dir"]
        self.star_output_dir = self.star_dir / star["star_output"]


    def _ensure_directories_exist(self):
        dirs = [
            self.sra_lists_dir,
            self.sra_output_dir,
            self.fastq_dir,
            self.split_fastq_dir,
            self.barcode_dir,
            self.star_genome_dir,
            self.star_output_dir,
            self.csv_log_dir,
            self.python_log_dir,
        ]
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)


    def _initialize_logging(self):
        log_manager = LogManager(self.csv_log_dir, self.python_log_dir)
        python_log_path = log_manager.generate_python_log()
        setup_logging(python_log_path)