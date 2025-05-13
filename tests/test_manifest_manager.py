import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..db.models import Base, JobModel
from ..manifest_manager import ManifestManager
from ..enums import StepStatus, PipelineStatus


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_get_or_create_job_creates_new(session):
    mm = ManifestManager(session)
    job = mm.get_or_create_job("TEST123", "somefile.fastq")
    assert isinstance(job, JobModel)
    assert job.accession == "TEST123"
    assert job.download_status == StepStatus.PENDING
    assert job.pipeline_status == PipelineStatus.PENDING


def test_get_or_create_job_returns_existing(session):
    mm = ManifestManager(session)
    first = mm.get_or_create_job("DUPLICATE", "a.txt")
    second = mm.get_or_create_job("DUPLICATE", "b.txt")
    assert first is second
    assert session.query(JobModel).filter_by(accession="DUPLICATE").count() == 1


def test_update_step_status_and_pipeline(session):
    mm = ManifestManager(session)
    mm.get_or_create_job("UPDATER", "file.fastq")

    mm.update_step_status("UPDATER", "download", StepStatus.SUCCESS)
    job = session.query(JobModel).filter_by(accession="UPDATER").first()
    assert job.download_status == StepStatus.SUCCESS
    assert job.pipeline_status == PipelineStatus.INPROGRESS

    for step in ["validate", "convert", "split", "align", "upload"]:
        mm.update_step_status("UPDATER", step, StepStatus.SUCCESS)

    session.refresh(job)
    assert job.pipeline_status == PipelineStatus.COMPLETED


def test_get_failed_jobs(session):
    mm = ManifestManager(session)
    mm.get_or_create_job("FAIL1", "f1")
    mm.get_or_create_job("OKAY", "f2")

    mm.update_step_status("FAIL1", "download", StepStatus.FAILED)
    mm.update_step_status("OKAY", "download", StepStatus.SUCCESS)

    failed = mm.get_failed_jobs("download")
    assert len(failed) == 1
    assert failed[0].accession == "FAIL1"


def test_all_jobs(session):
    mm = ManifestManager(session)
    mm.get_or_create_job("A", "f1")
    mm.get_or_create_job("B", "f2")
    all_jobs = mm.all_jobs()
    assert sorted([j.accession for j in all_jobs]) == ["A", "B"]