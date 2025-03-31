from sqlalchemy.orm import Session
from .db.models import StepStatus, PipelineStatus, Job as JobModel


class ManifestManager:
    def __init__(self, session: Session):
        self.session = session


    def get_or_create_job(self, accession: str, source_file: str) -> JobModel:
        job = self.session.query(JobModel).filter_by(accession=accession).first()
        if job:
            return job

        new_job = JobModel(
            accession=accession,
            source_file=source_file,
            pipeline_status=PipelineStatus.InProgress  # Default assumption
        )
        self.session.add(new_job)
        self.session.commit()
        return new_job


    def update_step_status(self, accession: str, step_name: str, status: StepStatus) -> None:
        job = self.session.query(JobModel).filter_by(accession=accession).first()
        if not job:
            return

        setattr(job, f"{step_name}_status", status)
        self._update_pipeline_status(job)
        self.session.commit()


    def _update_pipeline_status(self, job: JobModel) -> None:
        """Private helper to derive pipeline status from step statuses."""
        statuses = [
            job.download_status,
            job.validate_status,
            job.convert_status,
            job.upload_status
        ]

        if StepStatus.FAILED in statuses:
            job.pipeline_status = PipelineStatus.Failed
        elif all(s == StepStatus.SUCCESS or s == StepStatus.SKIPPED for s in statuses):
            job.pipeline_status = PipelineStatus.Completed
        else:
            job.pipeline_status = PipelineStatus.InProgress


    def get_failed_jobs(self, step_name: str) -> list[JobModel]:
        return self.session.query(JobModel).filter(
            getattr(JobModel, f"{step_name}_status") == StepStatus.FAILED
        ).all()


    def all_jobs(self) -> list[JobModel]:
        return self.session.query(JobModel).all()