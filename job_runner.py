from pathlib import Path

from .db.session import SessionLocal
from .manifest_manager import ManifestManager
from .job import Job


class JobRunner:
    def __init__(self, *, output_dir: Path, manifest_manager: ManifestManager, validator,
                status_checker, s3_handler, fastq_converter, star_runner, logger):     
        self.output_dir = output_dir
        self.manifest_manager = manifest_manager
        self.validator = validator
        self.status_checker = status_checker
        self.s3_handler = s3_handler
        self.fastq_converter = fastq_converter
        self.star_runner = star_runner
        self.logger = logger


    def run(self, accession: str, source_file: str) -> list[str]:
        fastq_files = []
        star_files = []
    
        # create local orm session per job executed
        # so no anoying detachedinstance error
        session = SessionLocal()
        try:
            manifest = ManifestManager(session)
            job = Job(
                accession=accession,
                source_file=source_file,
                output_dir=self.output_dir,
                validator=self.validator,
                status_checker=self.status_checker,
                manifest_manager=manifest,
                fastq_converter=self.fastq_converter,
                star_runner=self.star_runner,
                s3_handler=self.s3_handler
            )

            download_ok = job.run_download()
            job.run_validation()

            # if download successful + convert fastq flag = true
            # success -> clean sra
            if download_ok and self.fastq_converter:
                fastq_files = job.run_conversion()
                if fastq_files:
                    self._cleanup_sra_file(accession)

            
            # if fastq files exist and star runner = true
            # success ->  clean fastq
            if fastq_files and self.star_runner:
                star_files = job.run_alignment()
                if star_files:
                    self._cleanup_fastq_files(fastq_files)
            
            # if s3 handler flagged and star files mapped
            if self.s3_handler and star_files:
                for file in star_files:
                    job.run_upload(file)
                    self._cleanup_star_files([file]) # clean file on upload

            self._cleanup_directories(accession)
            # Extract plain log row BEFORE closing the session
            return job.to_log_row()

        finally:
            session.close()


    def _cleanup_sra_file(self, accession: str):
        sra_dir = self.output_dir / accession
        for suffix in [".sra", ".sra.lite"]:
            self._safe_unlink(sra_dir / f"{accession}{suffix}")
    

    def _cleanup_fastq_files(self, fastq_files: list[Path]):
        for file in fastq_files:
            self._safe_unlink(file)
    

    def _cleanup_star_files(self, star_files: list[Path]):
        for file in star_files:
            self._safe_unlink(file)


    def _safe_unlink(self, file: Path):
        try:
            if file.exists():
                file.unlink()
                self.logger.info(f"Deleted local file: {file}")
            else:
                self.logger.debug(f"File not found during cleanup: {file}")
        except Exception as e:
            self.logger.warning(f"Failed to delete {file}: {e}")


    def _cleanup_directories(self, accession: str):
        dirs = []

        if self.output_dir:
            dirs.append(self.output_dir / accession)
        if self.fastq_converter:
            dirs.append(self.fastq_converter.output_dir / accession)
        if self.star_runner:
            dirs.append(self.star_runner.star_output / accession)

        for dir_path in dirs:
            try:
                if dir_path.exists() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    self.logger.info(f"Removed empty accession folder: {dir_path}")
                else:
                    self.logger.debug(f"Skipped non-empty or missing dir: {dir_path}")
            except Exception as e:
                self.logger.warning(f"Failed to delete folder {dir_path}: {e}")