from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Dict

from .enums import ValidationStatus, DownloadStatus, ConversionStatus, SplitStatus


@dataclass
class DownloadResult:
    status: DownloadStatus
    message: str = ""


@dataclass
class ValidationResult:
    status: ValidationStatus
    message: str = ""


@dataclass
class FASTQConversionResult:
    status: ConversionStatus
    output_files: List[Path]
    error_message: Optional[str] = None


@dataclass
class SplitResult:
    status: SplitStatus
    output_files: List[Path]
    summary: Dict
    error_message: Optional[str] = None
