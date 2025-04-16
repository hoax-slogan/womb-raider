from pathlib import Path
import logging
import subprocess
from enum import Enum
from typing import Optional

from .validators import SRAValidator
from .status_checker import DownloadStatusChecker
from .fastq_converter import FASTQConverter
from .star_runner import STARRunner
from .aws_handler import S3Handler
from .manifest_manager import ManifestManager
from .db.models import StepStatus

from .enums import PipelineStep


logger = logging.getLogger(__name__)


class Job:
    def __init__(
        self,
        accession: str,
        source_file: str,
        output_dir: Path,
        validator: SRAValidator,
        status_checker: DownloadStatusChecker,
        manifest_manager: ManifestManager,
        fastq_converter: Optional[FASTQConverter] = None,
        s3_handler: Optional[S3Handler] = None,
        star_runner: Optional[STARRunner] = None,
    ):
        self.accession = accession
        self.source_file = source_file
        self.output_dir = output_dir
        self.validator = validator
        self.status_checker = status_checker
        self.manifest_manager = manifest_manager
        self.fastq_converter = fastq_converter
        self.s3_handler = s3_handler
        self.star_runner = star_runner

        job_record = self.manifest_manager.get_or_create_job(
            accession=self.accession,
            source_file=self.source_file
        )

        self.status = job_record  # still useful in case you ever rebind it
        self.download_status = job_record.download_status
        self.validate_status = job_record.validate_status
        self.convert_status = job_record.convert_status
        self.upload_status = job_record.upload_status
        self.pipeline_status = job_record.pipeline_status
        
        logger.info(f"Initialized job from DB: {self.accession}")


    def run_download(self):
        file_exists = self.status_checker.check_status(self.accession) == "Already Exists"

        if file_exists:
            logger.info(f"{self.accession} already exists. Skipping download.")
            self._update_status(PipelineStep.DOWNLOAD, StepStatus.SKIPPED)
        else:
            logger.info(f"Downloading {self.accession}...")

            try:
                result = subprocess.run(
                    ["prefetch", "--max-size", "100G", "-O", str(self.output_dir), self.accession],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                )
                logger.debug(f"{self.accession} prefetch stdout: {result.stdout.strip()}")
                logger.debug(f"{self.accession} prefetch stderr: {result.stderr.strip()}")

            except subprocess.CalledProcessError as e:
                logger.error(f"Download failed for {self.accession}: {e.stderr.strip()}")
                self._update_status(PipelineStep.DOWNLOAD, StepStatus.FAILED)
                return False

        # Confirm the download (even if it was skipped)
        confirmation = self.status_checker.confirm_download(self.accession)
        if confirmation == "Download OK!":
            self._update_status(PipelineStep.DOWNLOAD, StepStatus.SUCCESS)
            return True
        else:
            logger.warning(f"Sanity check failed: no downloaded file found for {self.accession}")
            self._update_status(PipelineStep.DOWNLOAD, StepStatus.FAILED)
            return False


    def run_validation(self):
        result = self.validator.validate(self.accession)

        if result.strip() == "Valid":
            logger.info(f"Marking {self.accession} as SUCCESS")
            self._update_status(PipelineStep.VALIDATE, StepStatus.SUCCESS)
        else:
            logger.warning(f"Marking {self.accession} as FAILED")
            self._update_status(PipelineStep.VALIDATE, StepStatus.FAILED)

        return result


    def run_conversion(self) -> list[Path]:
        if self.fastq_converter:
            success = self.fastq_converter.convert(self.accession)

            if success:
                self._update_status(PipelineStep.CONVERT, StepStatus.SUCCESS)

                fastq_prefix = self.fastq_converter.output_dir / self.accession
                r1 = fastq_prefix.with_name(f"{fastq_prefix.name}_1.fastq")
                r2 = fastq_prefix.with_name(f"{fastq_prefix.name}_2.fastq")

                output_files = [r1, r2] if r1.exists() and r2.exists() else []
                return output_files

            else:
                self._update_status(PipelineStep.CONVERT, StepStatus.FAILED)
                
        return []
    

    def run_alignment(self) -> Path:
        if not self.fastq_converter:
            raise RuntimeError("FASTQ converter must run before alignment.")

        try:
            fastq_paths = self.fastq_converter.get_fastq_paths(self.accession)

            star_runner = STARRunner(
                genome_dir=self.genome_dir,
                output_dir=self.output_dir / self.accession,
                output_prefix=f"STAR_{self.accession}_",
                threads=self.star_threads
            )

            result_path = star_runner.align(self.accession, fastq_paths)
            self._update_status(PipelineStep.ALIGN, StepStatus.SUCCESS)
            return result_path

        except Exception as e:
            logger.error(f"STAR alignment failed for {self.accession}: {e}")
            self._update_status(PipelineStep.ALIGN, StepStatus.FAILED)
            return None


    def run_upload(self, local_file: Path):
        if self.s3_handler:
            try:
                self.s3_handler.upload_file(local_file)
                self._update_status(PipelineStep.UPLOAD, StepStatus.SUCCESS)
            except Exception as e:
                logger.warning(f"S3 upload failed for {self.accession}: {e}")
                self._update_status(PipelineStep.UPLOAD, StepStatus.FAILED)


    def _update_status(self, step: PipelineStep, status: StepStatus):
        setattr(self.status, f"{step.value}_status", status)
        self.manifest_manager.update_step_status(self.accession, step.value, status)
        logger.debug(f"Updated status: {step.value} = {status.value} for {self.accession}")


    def to_log_row(self):
        return [
            self.accession,
            self.status.download_status.value,
            self.status.validate_status.value,
            self.status.convert_status.value,
            self.status.align_status.value,
            self.status.upload_status.value,
            str(self.source_file),
        ]