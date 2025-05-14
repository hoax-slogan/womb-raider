from pathlib import Path
import time
import subprocess
import logging

from .enums import DownloadStatus, DownloadCheckStatus, DownloadConfirmationStatus
from .tool_results import DownloadResult
from .status_checker import DownloadStatusChecker


class SRADownloader:
    def __init__(self, output_dir: Path, status_checker: DownloadStatusChecker, max_retries: int, delay: int = 5):
        self.output_dir = output_dir
        self.status_checker = status_checker
        self.max_retries = max_retries
        self.delay = delay
        self.logger = logging.getLogger(__name__)


    def download(self, accession: str) -> DownloadResult:
        if self.status_checker.check_status(accession) == DownloadCheckStatus.EXISTS:
            self.logger.info(f"{accession} already exists. Skipping download.")
            return DownloadResult(DownloadStatus.SKIPPED)

        self.logger.info(f"Downloading {accession}...")

        for attempt in range(1, self.max_retries + 1):
            cmd = self._build_prefetch_command(accession)
            try:
                result = subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=self.output_dir
                    )    

                self.logger.debug(f"{accession} prefetch stdout: {result.stdout.strip()}")
                self.logger.debug(f"{accession} prefetch stderr: {result.stderr.strip()}")
                break

            except subprocess.CalledProcessError as e:
                error = e.stderr.strip()
                self.logger.warning(f"Attempt {attempt}/{self.max_retries} failed for {accession}: {error}")
                if attempt == self.max_retries:
                    return DownloadResult(DownloadStatus.FAILED, error)
                time.sleep(self.delay)

        # Confirm the download (even if it was skipped)
        confirmation = self.status_checker.confirm_download(accession)
        if confirmation == DownloadConfirmationStatus.OK:
            return DownloadResult(DownloadStatus.SUCCESS)
        else:
            msg = "Sanity check failed: no downloaded file found"
            self.logger.warning(f"{accession}: {msg}")
            return DownloadResult(DownloadStatus.FAILED, msg)
        

    def _build_prefetch_command(self, accession: str) -> list[str]:
        """Build the `prefetch` command for a given accession."""
        return [
            "prefetch",
            "--max-size", "200G",
            "-O", str(self.output_dir),
            accession,
        ]