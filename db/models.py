from sqlalchemy import Column, String, Enum, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime, timezone
import enum


Base = declarative_base()


# tracks status of job lifecycle
class PipelineStatus(str, enum.Enum):
    PENDING = "Pending"
    INPROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"


# tracks status of every step completed
class StepStatus(str, enum.Enum):
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILED = "Failed"
    SKIPPED = "Skipped"


class JobModel(Base):
    __tablename__ = "jobs"

    accession = Column(String, primary_key=True)
    source_file = Column(String)

    download_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    validate_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    convert_status = Column(Enum(StepStatus), default=StepStatus.PENDING)
    upload_status = Column(Enum(StepStatus), default=StepStatus.PENDING)

    pipeline_status = Column(Enum(PipelineStatus), default=PipelineStatus.PENDING)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))