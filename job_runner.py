from pathlib import Path

from .db.session import SessionLocal
from .manifest_manager import ManifestManager
from .job import Job


class JobRunner:
    def __init__(self, *, output_dir: Path, manifest_manager: ManifestManager, validator,
                status_checker, s3_handler, fastq_converter, cleanup_local, logger):     
        self.output_dir = output_dir
        self.manifest_manager = manifest_manager
        self.validator = validator
        self.status_checker = status_checker
        self.s3_handler = s3_handler
        self.fastq_converter = fastq_converter
        self.cleanup_local = cleanup_local
        self.logger = logger


    def run(self, accession: str, source_file: str) -> list[str]:
        fastq_files = []
    
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
                s3_handler=self.s3_handler
            )

            download_ok = job.run_download()
            job.run_validation()

            # if download successful + convert fastq flag = true
            if download_ok and self.fastq_converter:
                fastq_files = job.run_conversion()
            
            # if s3 handler flagged and fast_q files converted
            if self.s3_handler and fastq_files:
                for file in fastq_files:
                    job.run_upload(file)
                    self.logger.info(f"Uploaded {file.name} to S3")

            # clean those cups and those spoons
            if self.cleanup_local:
                self._cleanup_files(accession, fastq_files)


            # Extract plain log row BEFORE closing the session
            return job.to_log_row()

        finally:
            session.close()
    

    def _cleanup_files(self, accession: str, fastq_files: list[Path]):
        sra_file = self.output_dir / accession / f"{accession}.sra"
        all_files = fastq_files + [sra_file]

        for file in all_files:
            try:
                if file.exists():
                    file.unlink()
                    self.logger.info(f"Deleted local file: {file}")
                else:
                    self.logger.debug(f"File not found during cleanup: {file}")
            except Exception as e:
                self.logger.warning(f"Failed to delete {file}: {e}")

        # scrub directory after files
        self._cleanup_directory(accession)


    def _cleanup_directory(self, accession: str):
        accession_dir = self.output_dir / accession

        try:
            if accession_dir.exists() and not any(accession_dir.iterdir()):
                accession_dir.rmdir()
                self.logger.info(f"Removed empty accession folder: {accession_dir}")
            else:
                self.logger.debug(f"Directory not empty, skipping removal: {accession_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to delete folder {accession_dir}: {e}")