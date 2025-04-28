from enum import Enum


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
