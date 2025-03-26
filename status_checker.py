from pathlib import Path
from typing import List


class DownloadStatusChecker:
    def __init__(self, output_dir: Path, extensions: List[str] = [".sra", ".sralite"]):
        self.output_dir = output_dir
        self.extensions = extensions

    def check_status(self, accession: str) -> str:
        """
        Check if any valid file exists for the given accession.
        """
        if self._file_exists(accession):
            return "Already Exists"

    def confirm_download(self, accession: str) -> str:
        """
        Check if the download succeeded based on file presence.
        """
        if self._file_exists(accession):
            return "Download OK!"
        return "Download Failed"
    
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
