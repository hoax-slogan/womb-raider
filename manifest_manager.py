from sqlalchemy.orm import Session
import logging

from .db.models import JobModel
from .enums import StepStatus, PipelineStatus


logger = logging.getLogger(__name__)


class ManifestManager:
    def __init__(self, session: Session):
        self.session = session


    def get_or_create_job(self, accession: str, source_file: str) -> JobModel:
        job = self.session.query(JobModel).filter_by(accession=accession).first()
        if job:
            logger.debug(f"Found existing job: {accession}")
            return job

        logger.debug(f"Creating new job: {accession}")
        new_job = JobModel(
            accession=accession,
            source_file=str(source_file),
            download_status=StepStatus.PENDING, # pending = default assumption
            validate_status=StepStatus.PENDING,
            convert_status=StepStatus.PENDING,
            align_status=StepStatus.PENDING,
            upload_status=StepStatus.PENDING,
            pipeline_status=PipelineStatus.PENDING,
        )
        self.session.add(new_job)
        self.session.commit()
        return new_job


    def update_step_status(self, accession: str, step_name: str, status: StepStatus) -> None:
        job = self.session.query(JobModel).filter_by(accession=accession).first()
        if not job:
            logger.warning(f"Tried to update status for unknown accession: {accession}")
            return

        setattr(job, f"{step_name}_status", status)
        self._update_pipeline_status(job)
        logger.debug(f"Before commit: {accession} - {step_name}_status = {status}")
        self.session.commit()
        logger.debug(f"Committed update for {accession}: {step_name} = {status}")


    def _update_pipeline_status(self, job: JobModel) -> None:
        """Private helper to derive pipeline status from step statuses."""
        statuses = [
            job.download_status,
            job.validate_status,
            job.convert_status,
            job.align_status,
            job.upload_status
        ]

        if StepStatus.FAILED in statuses:
            job.pipeline_status = PipelineStatus.FAILED
        elif all(s == StepStatus.SUCCESS or s == StepStatus.SKIPPED for s in statuses):
            job.pipeline_status = PipelineStatus.COMPLETED
        else:
            job.pipeline_status = PipelineStatus.INPROGRESS


    def get_failed_jobs(self, step_name: str) -> list[JobModel]:
        return self.session.query(JobModel).filter(
            getattr(JobModel, f"{step_name}_status") == StepStatus.FAILED
        ).all()


    def all_jobs(self) -> list[JobModel]:
        return self.session.query(JobModel).all()