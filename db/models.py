from sqlalchemy import Column, String, Enum, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone
import enum


Base = declarative_base()


class PipelineStatus(str, enum.Enum):
    PENDING = "Pending"
    INPROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"


class StepStatus(str, enum.Enum):
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILED = "Failed"
    SKIPPED = "Skipped"


class Job(Base):
    __tablename__ = "jobs"

    accession = Column(String, primary_key=True)
    source_file = Column(String)

    download_status = Column(Enum(StepStatus), default=StepStatus.Pending)
    validate_status = Column(Enum(StepStatus), default=StepStatus.Pending)
    convert_status = Column(Enum(StepStatus), default=StepStatus.Pending)
    upload_status = Column(Enum(StepStatus), default=StepStatus.Pending)

    pipeline_status = Column(Enum(PipelineStatus), default=PipelineStatus.Pending)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))