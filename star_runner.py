import subprocess
import logging
from pathlib import Path
from typing import List


class STARRunner:
    def __init__(self, *, star_genome_dir: Path, star_output_dir: Path, barcode_whitelist: Path = None,
        threads, cb_start: int = None, cb_len: int = None, umi_start: int = None,
        umi_len: int = None
    ):
        self.star_genome_dir = star_genome_dir
        self.star_output_dir = star_output_dir
        self.barcode_whitelist = barcode_whitelist
        self.threads = threads
        self.cb_start = cb_start
        self.cb_len = cb_len
        self.umi_start = umi_start
        self.umi_len = umi_len
        self.logger = logging.getLogger(__name__)


    def align(self, accession: str, fastq_files: List[Path]) -> List[Path]:
        """Run STAR on paired-end FASTQ files."""
        if len(fastq_files) != 2:
            raise ValueError("STARRunner expects paired-end FASTQ files.")

        output_prefix = self.star_output_dir / f"{accession}_"
        cmd = self._build_star_command(fastq_files, output_prefix)

        self.logger.info(f"Running STAR for {accession}")
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.star_output_dir)

        if result.returncode != 0:
            self.logger.error(f"STAR failed for {accession}:\n{result.stderr}")
            raise RuntimeError(f"STAR alignment failed for {accession}")

        self.logger.info(f"STAR completed for {accession}")

        output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*"))
        self.logger.debug(f"Detected STAR output files: {[f.name for f in output_files]}")
        return output_files


    def _build_star_command(self, fastq_files: List[Path], output_prefix: Path) -> List[str]:
        """Build the STAR command."""
        cmd = [
            "STAR",
            "--genomeDir", str(self.star_genome_dir),
            "--readFilesIn", str(fastq_files[0]), str(fastq_files[1]),
            "--runThreadN", str(self.threads),
            "--outFileNamePrefix", str(output_prefix) + "/",
            "--outSAMtype", "BAM", "SortedByCoordinate",
            "--readFilesCommand", "cat",
        ]

        if all(p is not None for p in [self.cb_start, self.cb_len, self.umi_start, self.umi_len]):
            cmd.extend([
                "--soloType", "CB_UMI_Simple",
                "--soloCBstart", str(self.cb_start),
                "--soloCBlen", str(self.cb_len),
                "--soloUMIstart", str(self.umi_start),
                "--soloUMIlen", str(self.umi_len),
                "--soloCBwhitelist", str(self.barcode_whitelist) if self.barcode_whitelist else "None",
            ])

        return cmd