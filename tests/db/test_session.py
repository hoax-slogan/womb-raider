import pytest
from sqlalchemy.orm import Session

from ...db.session import get_engine, get_session_maker
from ...db.models import Base, JobModel

from ...enums import StepStatus, PipelineStatus


@pytest.fixture
def in_memory_db_url():
    return "sqlite:///:memory:"


def test_get_engine_returns_engine(in_memory_db_url):
    engine = get_engine(in_memory_db_url)
    assert str(engine.url) == in_memory_db_url


def test_get_session_maker_returns_session(in_memory_db_url):
    SessionLocal = get_session_maker(in_memory_db_url)
    session = SessionLocal()
    assert isinstance(session, Session)


def test_session_can_create_and_query_job(in_memory_db_url):
    engine = get_engine(in_memory_db_url)
    SessionLocal = get_session_maker(engine=engine)

    # Create schema
    Base.metadata.create_all(bind=engine)

    # Insert and retrieve row
    session = SessionLocal()
    job = JobModel(
        accession="TEST123",
        source_file="source.fastq",
        download_status=StepStatus.SUCCESS,
        pipeline_status=PipelineStatus.PENDING,
    )
    session.add(job)
    session.commit()

    result = session.query(JobModel).filter_by(accession="TEST123").first()
    assert result is not None
    assert result.accession == "TEST123"
    assert result.download_status == StepStatus.SUCCESS