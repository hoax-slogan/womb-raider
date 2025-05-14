from dataclasses import dataclass
from typing import Optional

from ..downloader import SRADownloader
from ..validator import SRAValidator
from ..fastq_converter import FASTQConverter
from ..fastq_splitter import FASTQSplitter
from ..star_runner import STARRunner
from ..s3_handler import S3Handler


@dataclass
class ToolBundle:
    downloader: Optional[SRADownloader]
    validator: Optional[SRAValidator]
    converter: Optional[FASTQConverter]
    splitter: Optional[FASTQSplitter]
    aligner: Optional[STARRunner]
    uploader: Optional[S3Handler]