from pathlib import Path
import logging
import subprocess
from enum import Enum
from typing import Optional
from .validators import SRAValidator
from .status_checker import DownloadStatusChecker
from .fastq_converter import FASTQConverter
from .aws_handler import S3Handler


logger = logging.getLogger(__name__)


class StepStatus(str, Enum):
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILED = "Failed"
    SKIPPED = "Skipped"


class JobStatus:
    def __init__(self):
        self.download = StepStatus.PENDING
        self.validate = StepStatus.PENDING
        self.convert = StepStatus.PENDING
        self.upload = StepStatus.PENDING
    
    def as_dict(self):
        return {
            "download": self.download,
            "validate": self.validate,
            "convert": self.convert,
            "upload": self.upload,
        }
    

class Job:
    def __init__(self, accession: str, source_file: str, output_dir: Path,
            validator: SRAValidator, status_checker: DownloadStatusChecker,
            fastq_converter: Optional[FASTQConverter] = None,
            s3_handler: Optional[S3Handler] = None,
    ):
        self.accession = accession
        self.source_file=source_file
        self.output_dir=output_dir
        self.validator = validator
        self.status_checker = status_checker
        self.fastq_converter = fastq_converter
        self.s3_handler = s3_handler
        self.status = JobStatus()
    

    def run_download(self):
        if self.status_checker.check_status(self.accession) == "Already Exists":
            logger.info(f"{self.accession} already exists. Skipping download.")
            self.status.download = StepStatus.SKIPPED
            return True

        logger.info(f"Downloading {self.accession}...")
        
        try:
            subprocess.run(
                ["prefetch", "--max-size", "100G", "-O", str(self.output_dir), self.accession],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            if self.status_checker.confirm_download(self.accession) == "Download OK!":
                self.status.download = StepStatus.SUCCESS
                return True
            else:
                self.status.download = StepStatus.FAILED

        except subprocess.CalledProcessError as e:
            logger.error(f"Download failed for {self.accession}: {e}")
            self.status.download = StepStatus.FAILED

        return False
    
    
    def run_validation(self):
        result = self.validator.validate(self.accession)
        if "Valid" in result:
            self.status.validate = StepStatus.SUCCESS
        else:
            self.status.validate = StepStatus.FAILED
        return result
    

    def run_conversion(self):
        if self.fastq_converter:
            if self.fastq_converter.convert(self.accession):
                self.status.convert = StepStatus.SUCCESS
            else:
                self.status.convert = StepStatus.FAILED
    

    def run_upload(self, local_file: Path):
        if self.s3_handler:
            try:
                self.s3_handler.upload_file(local_file)
                self.status.upload = StepStatus.SUCCESS
            except Exception as e:
                logger.warning(f"S3 upload failed for {self.accession}: {e}")
                self.status.upload = StepStatus.FAILED
