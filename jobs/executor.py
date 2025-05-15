import logging

from .job import Job
from .tool_bundle import ToolBundle
from ..manifest_manager import ManifestManager
from ..enums import PipelineStep, StepStatus
from ..tool_results import *


class JobExecutor:
    def __init__(self, job: Job, tools: ToolBundle, manifest: ManifestManager):
        self.job = job
        self.tools = tools
        self.manifest = manifest
        self.logger = logging.getLogger(__name__)


    def run_download(self) -> StepStatus:
        downloader = self.tools.downloader

        if not downloader:
            self.logger.warning("Downloader not provided.")
            return self._update_status(PipelineStep.DOWNLOAD, StepStatus.SKIPPED)

        result = downloader.download(self.job.accession)

        if result.status == DownloadStatus.FAILED:
            if result.message:
                self.logger.error(f"Download failed for {self.job.accession}: {result.message}")
            return self._update_status(PipelineStep.DOWNLOAD, StepStatus.FAILED)

        elif result.status == DownloadStatus.SKIPPED:
            self.logger.info(f"SRA file already downloaded for {self.job.accession}")
            return self._update_status(PipelineStep.DOWNLOAD, StepStatus.SKIPPED)

        elif result.status == DownloadStatus.SUCCESS:
            self.logger.info(f"{self.job.accession} successfully downloaded!")
            return self._update_status(PipelineStep.DOWNLOAD, StepStatus.SUCCESS)

        else:
            self.logger.error(f"Unexpected download status for {self.job.accession}: {result.status}")
            return self._update_status(PipelineStep.DOWNLOAD, StepStatus.FAILED)


    def run_validation(self) -> StepStatus:
        validator = self.tools.validator

        if not validator:
            self.logger.warning("Validator not provided.")
            return self._update_status(PipelineStep.VALIDATE, StepStatus.SKIPPED)

        result = validator.validate(self.job.accession)

        if result.status == ValidationStatus.VALID:
            self.logger.info(f"{self.job.accession} successfully validated!")
            return self._update_status(PipelineStep.VALIDATE, StepStatus.SUCCESS)
        else:
            self.logger.error(f"Download failed for {self.job.accession}")
            return self._update_status(PipelineStep.VALIDATE, StepStatus.FAILED)
        
    
    def run_conversion(self) -> tuple[StepStatus, list[Path]]:
        converter = self.tools.converter

        if not converter:
            self.logger.warning(f"No FASTQ converter provided; skipping conversion for {self.job.accession}")
            return self._update_status(PipelineStep.CONVERT, StepStatus.SKIPPED), []

        result = converter.convert(self.job.accession)

        if result.status == ConversionStatus.SUCCESS:
            self.logger.info(f"FASTQ conversion succeeded for {self.job.accession}")
            return self._update_status(PipelineStep.CONVERT, StepStatus.SUCCESS), result.output_files

        elif result.status == ConversionStatus.FAILED:
            self.logger.error(f"FASTQ conversion failed for {self.job.accession}: {result.error_message}")
            return self._update_status(PipelineStep.CONVERT, StepStatus.FAILED), []

        else:
            self.logger.error(f"Unexpected conversion status for {self.job.accession}: {result.status}")
            return self._update_status(PipelineStep.CONVERT, StepStatus.FAILED), []
    

    def run_splitter(self) -> tuple[StepStatus, list[Path], dict]:
        splitter = self.tools.splitter

        if not splitter:
            self.logger.warning(f"No FASTQ splitter provided; skipping split for {self.job.accession}")
            return self._update_status(PipelineStep.SPLIT, StepStatus.SKIPPED), [], {}

        try:
            result = splitter.split_fastqs()

            if result.status == StepStatus.SUCCESS:
                self.logger.info(f"FASTQ splitting succeeded for {self.job.accession} ({len(result.output_files)} files created)")
                return self._update_status(PipelineStep.SPLIT, StepStatus.SUCCESS), result.output_files, result.summary

            elif result.status == StepStatus.FAILED:
                self.logger.warning(f"FASTQ splitting failed for {self.job.accession}: {result.error_message}")
                return self._update_status(PipelineStep.SPLIT, StepStatus.FAILED), [], result.summary or {}

            else:
                self.logger.error(f"Unexpected splitting status for {self.job.accession}: {result.status}")
                return self._update_status(PipelineStep.SPLIT, StepStatus.FAILED), [], {}

        except Exception as e:
            self.logger.error(f"Exception during FASTQ splitting for {self.job.accession}: {e}")
            return self._update_status(PipelineStep.SPLIT, StepStatus.FAILED), [], {}


    def _update_status(self, step: PipelineStep, status: StepStatus):
        # updates sql object
        setattr(self.job.status, f"{step.value}_status", status)
        # updates job
        setattr(self, f"{step.value}_status", status)
        self.manifest.update_step_status(self.job.accession, step.value, status)
        self.logger.debug(f"Updated status: {step.value} = {status.value} for {self.job.accession}")