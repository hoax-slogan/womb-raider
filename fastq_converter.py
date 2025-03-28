import subprocess
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


class FASTQConverter:
    def __init__(self, output_dir: Path, threads: int = 4):
        self.output_dir = output_dir
        self.threads = threads

    def convert(self, accession: str) -> bool:
        """Run fasterq-dump on a given accession."""
        logger.info(f"Converting {accession} to FASTQ...")

        cmd = [
            "fasterq-dump", accession,
            "--outdir", str(self.output_dir),
            "--threads", str(self.threads),
        ]

        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            logger.info(f"{accession} FASTQ conversion complete.")
            return True
        except subprocess.CalledProcessError as e:
            logger.warning(f"FASTQ conversion failed for {accession}: {e}")
            return False