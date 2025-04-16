import subprocess
from pathlib import Path
import logging


class STARRunner:
    def __init__(self, *, genome_dir: Path, output_dir: Path, output_prefix: str, threads: int = 4):
        self.genome_dir = genome_dir
        self.output_dir = output_dir
        self.output_prefix = self.output_dir / output_prefix
        self.threads = threads
        self.logger = logging.getLogger(__name__)


    def align(self, accession: str, fastq_files: list[Path]) -> Path:
        if len(fastq_files) != 2:
            raise ValueError("STARRunner expects paired-end FASTQ files.")

        cmd = self._build_star_command(fastq_files)

        self.logger.info(f"Running STAR for {accession}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            self.logger.error(f"STAR failed for {accession}:\n{result.stderr}")
            raise RuntimeError(f"STAR alignment failed for {accession}")

        self.logger.info(f"STAR completed for {accession}")
        return self.output_prefix.with_name("STAR_Aligned.out.sam")


    def _build_star_command(self, fastq_files: list[Path]) -> list[str]:
        return [
            "STAR",
            "--genomeDir", str(self.genome_dir),
            "--readFilesIn", str(fastq_files[0]), str(fastq_files[1]),
            "--runThreadN", str(self.threads),
            "--outFileNamePrefix", str(self.output_prefix),
            "--outSAMtype", "SAM"
        ]