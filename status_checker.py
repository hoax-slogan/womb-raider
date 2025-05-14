from pathlib import Path
from typing import List

from .enums import DownloadCheckStatus, DownloadConfirmationStatus


class DownloadStatusChecker:
    def __init__(self, output_dir: Path, extensions: List[str] = [".sra", ".sralite"]):
        self.output_dir = output_dir
        self.extensions = extensions


    def check_status(self, accession: str) -> DownloadCheckStatus:
        """
        Check if any valid file exists for the given accession.
        """
        if self._file_exists(accession):
            return DownloadCheckStatus.EXISTS
        return DownloadCheckStatus.MISSING


    def confirm_download(self, accession: str) -> DownloadConfirmationStatus:
        """
        Confirm whether a download succeeded.
        """
        if self._file_exists(accession):
            return DownloadConfirmationStatus.OK
        return DownloadConfirmationStatus.FAILED
    
    
    def _file_exists(self, accession: str) -> bool:
        """
        Checks if any file for the given accession exists with the allowed extensions.
        Returns True if at least one valid file is found.
        """
        accession_dir = self.output_dir / accession
        for ext in self.extensions:
            file_path = accession_dir / f"{accession}{ext}"
            if file_path.exists():
                return True
        return False
