from pathlib import Path
import logging
import subprocess
from typing import Optional

from .validators import SRAValidator
from .status_checker import DownloadStatusChecker
from .fastq_converter import FASTQConverter
from .fastq_splitter import FastqSplitter
from .star_runner import STARRunner
from .s3_handler import S3Handler
from .manifest_manager import ManifestManager

from .enums import PipelineStep, StepStatus


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
        fastq_splitter: Optional[FastqSplitter] = None,
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
        self.fastq_splitter = fastq_splitter
        self.star_runner = star_runner
        self.s3_handler = s3_handler

        job_record = self.manifest_manager.get_or_create_job(
            accession=self.accession,
            source_file=self.source_file
        )

        self.status = job_record  # still useful in case you ever rebind it
        self.download_status = job_record.download_status
        self.validate_status = job_record.validate_status
        self.convert_status = job_record.convert_status
        self.split_status = job_record.split_status
        self.align_status = job_record.align_status
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
                    ["prefetch", "--max-size", "200G", "-O", str(self.output_dir), self.accession],
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
        if not self.fastq_converter:
            logger.warning(f"No FASTQ converter provided; skipping conversion for {self.accession}")
            self._update_status(PipelineStep.CONVERT, StepStatus.SKIPPED)
            return []

        success = self.fastq_converter.convert(self.accession)

        if success:
            self._update_status(PipelineStep.CONVERT, StepStatus.SUCCESS)
            r1, r2 = self.fastq_converter.get_fastq_paths(self.accession)
            output_files = [r1, r2] if r1.exists() and r2.exists() else []
            return output_files

        else:
            self._update_status(PipelineStep.CONVERT, StepStatus.FAILED)
            return []
    

    def run_splitter(self) -> tuple[list[Path], dict]:
        if not self.fastq_splitter:
            logger.warning(f"No FASTQ splitter provided; skipping split for {self.accession}")
            self._update_status(PipelineStep.SPLIT, StepStatus.SKIPPED)
            return [], {}

        try:
            split_fastqs, unmatched_barcodes = self.fastq_splitter.split_fastqs()

            if split_fastqs:
                self._update_status(PipelineStep.SPLIT, StepStatus.SUCCESS)
                logger.info(f"FASTQ splitting succeeded for {self.accession} ({len(split_fastqs)} files created)")
            else:
                self._update_status(PipelineStep.SPLIT, StepStatus.FAILED)
                logger.warning(f"No FASTQ output files found after splitting {self.accession}")
            
            if unmatched_barcodes:
                logger.warning(f"{len(unmatched_barcodes)} unmatched barcodes found during split for {self.accession}")

            return split_fastqs, unmatched_barcodes

        except Exception as e:
            logger.error(f"FASTQ splitting failed for {self.accession}: {e}")
            self._update_status(PipelineStep.SPLIT, StepStatus.FAILED)
            return [], {}
    

    def run_alignment(self) -> list[Path]:
        if not self.star_runner:
            logger.warning(f"Missing STAR runner; skipping alignment for {self.accession}")
            self._update_status(PipelineStep.ALIGN, StepStatus.SKIPPED)
            return []

        try:
            fastq_paths = self.fastq_converter.get_fastq_paths(self.accession)
            star_results = self.star_runner.align(self.accession, fastq_paths)
            self._update_status(PipelineStep.ALIGN, StepStatus.SUCCESS)
            return star_results

        except Exception as e:
            logger.error(f"STAR alignment failed for {self.accession}: {e}")
            self._update_status(PipelineStep.ALIGN, StepStatus.FAILED)
            return []


    def run_upload(self, local_file: Path):
        if not self.s3_handler:
            logger.warning(f"No S3 handler provided; skipping upload for {self.accession}")
            self._update_status(PipelineStep.UPLOAD, StepStatus.SKIPPED)
            return

        try:
            self.s3_handler.upload_file(local_file)
            self._update_status(PipelineStep.UPLOAD, StepStatus.SUCCESS)

        except Exception as e:
            logger.warning(f"S3 upload failed for {self.accession}: {e}")
            self._update_status(PipelineStep.UPLOAD, StepStatus.FAILED)


    def _update_status(self, step: PipelineStep, status: StepStatus):
        # updates sql object
        setattr(self.status, f"{step.value}_status", status)
        # updates job
        setattr(self, f"{step.value}_status", status)
        self.manifest_manager.update_step_status(self.accession, step.value, status)
        logger.debug(f"Updated status: {step.value} = {status.value} for {self.accession}")


    def to_log_row(self):
        return [
            self.accession,
            self.status.download_status.value,
            self.status.validate_status.value,
            self.status.convert_status.value,
            self.status.split_status.value,
            self.status.align_status.value,
            self.status.upload_status.value,
            str(self.source_file),
        ]