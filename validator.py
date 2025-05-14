from pathlib import Path
import subprocess
import logging

from .enums import ValidationStatus
from .tool_results import ValidationResult


class SRAValidator:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)

    def validate(self, accession: str) -> ValidationResult:
        sra_file = self.output_dir / accession / f"{accession}.sra"

        if not sra_file.exists():
            self.logger.info(f"{accession}: File Missing!")
            return ValidationResult(ValidationStatus.FILE_MISSING, "File does not exist")

        self.logger.info(f"RUNNING vdb-validate on: {sra_file}")
        result = subprocess.run(["vdb-validate", str(sra_file)], capture_output=True, text=True)

        if result.returncode == 0:
            self.logger.info(f"{accession}: Validation OK!")
            return ValidationResult(ValidationStatus.VALID)
        
        else:
            error = result.stderr.strip()
            self.logger.error(f"Validation failed for {accession}: {error}")
            return ValidationResult(ValidationStatus.INVALID, error)