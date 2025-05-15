from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class SubdirsConfig:
    lists: str
    output: str
    logs: str
    fastq: str
    split_fastq: str
    barcodes: str
    star: str


@dataclass
class LogsConfig:
    csv: str
    python: str
    split: str


@dataclass
class StarConfig:
    genome_dir: str
    star_output: str


@dataclass
class DemuxConfig:
    output_subdir: str
    entrez_email: str
    gsm_cache: str


@dataclass
class WombRaiderConfig:
    data_dir: str
    subdirs: SubdirsConfig
    logs: LogsConfig
    star: StarConfig
    demux: DemuxConfig

    # optional cli-override-able runtime values
    batch_size: int = 5
    threads: Union[int, str, None] = None
    max_retries: int = 5
    s3_bucket: Optional[str] = None
    s3_prefix: str = ""