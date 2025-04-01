from pathlib import Path
import subprocess
import logging


class SRAValidator:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)


    def validate(self, accession: str) -> str:
        sra_file = self.output_dir / accession / f"{accession}.sra"
        exists = sra_file.is_file()
        
        if not exists:
            self.logger.info(f"{accession}: File Missing!")
            return "File Missing"
        
        print(f"RUNNING vdb-validate on: {sra_file}")
        result = subprocess.run(["vdb-validate", str(sra_file)], capture_output=True, text=True)

        if result.returncode == 0:
            self.logger.info(f"{accession}: Validation OK!")
            return "Valid"
        else:
            error = result.stderr.strip()
            self.logger.error(f"Validation failed for {accession}: {error}")
            return f"Invalid: {result.stderr.strip()}"