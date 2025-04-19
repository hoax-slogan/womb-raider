import subprocess
from pathlib import Path
import logging


class STARRunner:
    def __init__(self, *, genome_dir: Path, star_output: Path, barcode_whitelist: Path = None,
                threads: int = 4, cb_start: int = None, cb_len: int = None,
                umi_start: int = None, umi_len: int = None,
    ):
        self.genome_dir = genome_dir
        self.star_output = star_output
        self.barcode_whitelist = barcode_whitelist
        self.threads = threads
        self.cb_start = cb_start
        self.cb_len = cb_len
        self.umi_start = umi_start
        self.umi_len = umi_len
        self.logger = logging.getLogger(__name__)


    def align(self, accession: str, fastq_files: list[Path]) -> Path:
        if len(fastq_files) != 2:
            raise ValueError("STARRunner expects paired-end FASTQ files.")

        output_prefix = self.star_output / f"{accession}_"
        cmd = self._build_star_command(fastq_files, output_prefix)

        self.logger.info(f"Running STAR for {accession}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            self.logger.error(f"STAR failed for {accession}:\n{result.stderr}")
            raise RuntimeError(f"STAR alignment failed for {accession}")

        self.logger.info(f"STAR completed for {accession}")
        
        # Dynamically find all output files starting with prefix
        output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*"))
        self.logger.debug(f"Detected STAR output files: {[f.name for f in output_files]}")
        return output_files


    def _build_star_command(self, fastq_files: list[Path], out_dir: Path) -> list[str]:
        cmd = [
            "STAR",
            "--genomeDir", str(self.genome_dir),
            "--readFilesIn", str(fastq_files[0]), str(fastq_files[1]),
            "--runThreadN", str(self.threads),
            "--outFileNamePrefix", str(out_dir) + "/",
            "--outSAMtype", "BAM", "SortedByCoordinate",
            "--readFilesCommand", "cat",
        ]

        # Add STARsolo flags only if any solo params are set
        solo_params = [self.cb_start, self.cb_len, self.umi_start, self.umi_len]
        if all(p is not None for p in solo_params):
            cmd.extend([
                "--soloType", "CB_UMI_Simple",
                "--soloCBstart", str(self.cb_start),
                "--soloCBlen", str(self.cb_len),
                "--soloUMIstart", str(self.umi_start),
                "--soloUMIlen", str(self.umi_len),
                "--soloCBwhitelist", str(self.barcode_whitelist) if self.barcode_whitelist else "None",
            ])

        return cmd
