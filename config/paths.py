from pathlib import Path

from .schema import WombRaiderConfig
from .path_structs import LogPaths


class ConfigPaths:
    def __init__(self, config: WombRaiderConfig, base_dir: Path):
        self.config = config
        self.base_dir = base_dir
        self._set_paths()


    def _path(self, *parts) -> Path:
        return self.base_dir.joinpath(*parts)


    def _set_paths(self):
        self.data_dir = self._path(self.config.data_dir)

        sub = self.config.subdirs
        self.sra_lists_dir = self.data_dir / sub.lists
        self.sra_output_dir = self.data_dir / sub.output
        self.logs_dir = self.data_dir / sub.logs
        self.fastq_dir = self.data_dir / sub.fastq
        self.split_fastq_dir = self.data_dir / sub.split_fastq
        self.barcode_dir = self.data_dir / sub.barcodes
        self.star_dir = self.data_dir / sub.star

        logs = self.config.logs
        self.csv_log_dir = self.logs_dir / logs.csv
        self.python_log_dir = self.logs_dir / logs.python
        self.split_log_dir = self.logs_dir / logs.split

        star = self.config.star
        self.star_genome_dir = self.star_dir / star.genome_dir
        self.star_output_dir = self.star_dir / star.star_output


    def get_all_directories(self):
        return [
            self.sra_lists_dir,
            self.sra_output_dir,
            self.fastq_dir,
            self.split_fastq_dir,
            self.barcode_dir,
            self.star_genome_dir,
            self.star_output_dir,
            self.csv_log_dir,
            self.python_log_dir,
            self.split_log_dir,
        ]
    
    
    @property
    def log_paths(self) -> LogPaths:
        return LogPaths(
            csv_log_dir=self.csv_log_dir,
            python_log_dir=self.python_log_dir,
            split_log_dir=self.split_log_dir
        )