from .enums import StepName


# ---- log_manager.py ----
CSV_HEADER = [
    "Accession",
    "Download Status",
    "Validation Status",
    "Convert Status",
    "Split Status",
    "Align Status",
    "Upload Status",
    "Source File"
]


# ---- manifest_manager.py ----
ALL_STEP_NAMES: list[StepName] = [
    "download",
    "validate",
    "convert",
    "split",
    "align",
    "upload"
]