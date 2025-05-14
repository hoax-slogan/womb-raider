from enum import Enum
from typing import Literal


# ----- db/models.py -----
# tracks status of job lifecycle
class PipelineStatus(str, Enum):
    PENDING = "Pending"
    INPROGRESS = "InProgress"
    COMPLETED = "Completed"
    FAILED = "Failed"


# tracks status of every step completed
class StepStatus(str, Enum):
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILED = "Failed"
    SKIPPED = "Skipped"


# ---- job.py ----
# tracks each step of the job
class PipelineStep(str, Enum):
    DOWNLOAD = "download"
    VALIDATE = "validate"
    SPLIT = "split"
    CONVERT = "convert"
    ALIGN = "align"
    UPLOAD = "upload"


# ---- manifest_manager.py ----
# tracks name of each step
StepName = Literal[
    "download",
    "validate",
    "convert",
    "split",
    "align",
    "upload"
]


# ---- downloader.py ----
class DownloadStatus(str, Enum):
    SUCCESS = "Success"
    FAILED = "Failed"
    SKIPPED = "Skipped"


# ---- status_checker.py ----
class DownloadCheckStatus(str, Enum):
    EXISTS = "Already Exists"
    MISSING = "Missing"


class DownloadConfirmationStatus(str, Enum):
    OK = "Download OK!"
    FAILED = "Download Failed"


# ---- validator.py ----
class ValidationStatus(str, Enum):
    VALID = "Valid"
    FILE_MISSING = "File Missing"
    INVALID = "Invalid"


# ---- fastq_converter.py ----
class ConversionStatus(str, Enum):
    SUCCESS = "Success"
    FAILED = "Failed"


# ---- fastq_splitter.py ----
class SplitStatus(str, Enum):
    SUCCESS = "Success"
    FAILED = "Failed"
    SKIPPED = "Skipped"