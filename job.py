from pathlib import Path
import logging
import subprocess
from enum import Enum, auto
from typing import Optional
from .validators import SRAValidator
from .status_checker import DownloadStatusChecker
from .fastq_converter import FASTQConverter
from .aws_handler import S3Handler
from .manifest_manager import ManifestManager


logger = logging.getLogger(__name__)


class PipelineStep(str, Enum):
    DOWNLOAD = "download"
    VALIDATE = "validate"
    CONVERT = "convert"
    UPLOAD = "upload"


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
            manifest_manager: ManifestManager,
            fastq_converter: Optional[FASTQConverter] = None,
            s3_handler: Optional[S3Handler] = None,
    ):
        self.accession = accession
        self.source_file=source_file
        self.output_dir=output_dir
        self.validator = validator
        self.status_checker = status_checker
        self.manifest_manager=manifest_manager
        self.fastq_converter = fastq_converter
        self.s3_handler = s3_handler
        self.status = JobStatus()

        existing = self.manifest_manager.get_job(self.accession)

        if existing:
            logger.info(f"Resuming job from DB: {self.accession}")
            self.status.download = existing.download_status
            self.status.validate = existing.validate_status
            self.status.convert = existing.convert_status
            self.status.upload = existing.upload_status
        else:
            self.manifest_manager.create_job(
                accession=self.accession,
                source_file=self.source_file
            )
    

    def run_download(self):

        if self.status_checker.check_status(self.accession) == "Already Exists":
            logger.info(f"{self.accession} already exists. Skipping download.")
            self._update_status("download", StepStatus.SKIPPED)
            return True

        logger.info(f"Downloading {self.accession}...")

        try:
            subprocess.run(
                ["prefetch", "--max-size", "100G", "-O", str(self.output_dir), self.accession],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            if self.status_checker.confirm_download(self.accession) == "Download OK!":
                self._update_status("download", StepStatus.SUCCESS)
                return True
            else:
                self._update_status("download", StepStatus.FAILED)

        except subprocess.CalledProcessError as e:
            logger.error(f"Download failed for {self.accession}: {e}")
            self._update_status("download", StepStatus.FAILED)

        return False
    
    
    def run_validation(self):
        result = self.validator.validate(self.accession)

        if "Valid" in result:
            self._update_status("validate", StepStatus.SUCCESS)
        else:
            self._update_status("validate", StepStatus.FAILED)
        return result
    

    def run_conversion(self):
        if self.fastq_converter:
            success = self.fastq_converter.convert(self.accession)

            if success:
                self._update_status("convert", StepStatus.SUCCESS)
            else:
                self._update_status("convert", StepStatus.FAILED)
    

    def run_upload(self, local_file: Path):
        if self.s3_handler:
            try:
                self.s3_handler.upload_file(local_file)
                self._update_status("upload", StepStatus.SUCCESS)
            except Exception as e:
                logger.warning(f"S3 upload failed for {self.accession}: {e}")
                self._update_status("upload", StepStatus.FAILED)
    

    def _update_status(self, step: PipelineStep, status: StepStatus):
        setattr(self.status, step.value, status)
        self.manifest_manager.update_step(self.accession, step.value, status)
