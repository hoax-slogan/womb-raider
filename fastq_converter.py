import subprocess
import logging
from pathlib import Path
from typing import List


class FASTQConverter:
    def __init__(self, *, output_dir: Path, threads: int = 4):
        self.output_dir = output_dir
        self.threads = threads
        self.logger = logging.getLogger(__name__)


    def convert(self, accession: str) -> bool:
        """Run fasterq-dump on a given accession."""
        self.logger.info(f"Converting {accession} to FASTQ...")
        cmd = self._build_fasterq_command(accession)

        try:
            subprocess.run(cmd, capture_output=True, text=True, cwd=self.output_dir, check=True)
            self.logger.info(f"FASTQ conversion completed for {accession}")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.warning(f"FASTQ conversion failed for {accession}:\n{e.stderr}")
            return False


    def get_fastq_paths(self, accession: str) -> List[Path]:
        """Return the expected R1 and R2 FASTQ file paths."""
        prefix = self.output_dir / accession
        r1 = prefix.with_name(f"{prefix.name}_1.fastq")
        r2 = prefix.with_name(f"{prefix.name}_2.fastq")
        return [r1, r2]


    def _build_fasterq_command(self, accession: str) -> List[str]:
        """Builds the fasterq-dump command."""
        return [
            "fasterq-dump", accession,
            "--outdir", str(self.output_dir),
            "--threads", str(self.threads),
        ]