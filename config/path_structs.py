from dataclasses import dataclass
from pathlib import Path


@dataclass
class LogPaths:
    csv_log_dir: Path
    python_log_dir: Path
    split_log_dir: Path