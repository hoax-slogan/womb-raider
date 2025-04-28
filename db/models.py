from sqlalchemy import Column, String, Enum, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime, timezone

from ..enums import StepStatus, PipelineStatus


Base = declarative_base()


class JobModel(Base):
    __tablename__ = "jobs"

    accession = Column(String, primary_key=True)
    source_file = Column(String)

    download_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    validate_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    split_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    convert_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    align_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    upload_status = Column(Enum(StepStatus), default=StepStatus.PENDING)

    pipeline_status = Column(Enum(PipelineStatus), default=PipelineStatus.PENDING)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))