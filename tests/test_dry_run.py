from pathlib import Path
import subprocess
from unittest.mock import MagicMock
from multiprocessing.dummy import Pool as ThreadPool
from ..orchestrator import SRAOrchestrator
from ..validators import SRAValidator
from ..fastq_converter import FASTQConverter
from ..log_manager import LogManager
from ..status_checker import DownloadStatusChecker
from ..config import Config
from ..log_setup import setup_logging
from ..db.session import SessionLocal
from ..manifest_manager import ManifestManager



# Simulated subprocess result for prefetch
class FakeSubprocessResult:
    def __init__(self):
        self.returncode = 0
        self.stdout = "Fake prefetch stdout output"
        self.stderr = ""


# Monkey-patched subprocess.run to avoid real downloads
def fake_subprocess_run(*args, **kwargs):
    return FakeSubprocessResult()


def main():
    # --- SETUP ---
    cfg = Config()
    cfg.ensure_directories_exist()
    setup_logging(cfg.PYTHON_LOG_DIR / "dry_run.log")

    session = SessionLocal()
    manifest = ManifestManager(session)

    # Create fake SRA list file with 2 accessions
    fake_list = cfg.SRA_LISTS_DIR / "fake_list.txt"
    fake_list.write_text("SRR_FAKE001\nSRR_FAKE002\n")

    # Simulate a .sra file already existing for SRR_FAKE001
    fake_existing = cfg.SRA_OUTPUT_DIR / "SRR_FAKE001"
    fake_existing.mkdir(parents=True, exist_ok=True)
    (fake_existing / "SRR_FAKE001.sra").touch()

    # --- PATCH EXTERNAL DEPENDENCIES ---

    # Monkey patch subprocess
    subprocess.run = fake_subprocess_run

    # Patch status checker methods to simulate realistic outcomes
    DownloadStatusChecker.check_status = lambda self, acc: "Already Exists" if acc == "SRR_FAKE001" else "Not Found"
    DownloadStatusChecker.confirm_download = lambda self, acc: (
        "Download OK!" if acc == "SRR_FAKE001" else "Download Failed"
    )

    # Pretend conversion always succeeds
    FASTQConverter.convert = lambda self, acc: True

    # Pretend all files exist and print instead of deleting
    Path.exists = lambda self: True
    # Path.unlink = lambda self: print(f"[DRY RUN] Would delete: {self}")

    # --- INITIALIZE COMPONENTS ---

    log_manager = LogManager(
        csv_log_dir=cfg.CSV_LOG_DIR,
        python_log_dir=cfg.PYTHON_LOG_DIR
    )

    validator = SRAValidator(output_dir=cfg.SRA_OUTPUT_DIR)
    status_checker = DownloadStatusChecker(output_dir=cfg.SRA_OUTPUT_DIR)
    csv_log_path = log_manager.generate_csv_log()

    orchestrator = SRAOrchestrator(
        output_dir=cfg.SRA_OUTPUT_DIR,
        sra_lists_dir=cfg.SRA_LISTS_DIR,
        csv_log_path=csv_log_path,
        fastq_file_dir=cfg.FASTQ_DIR,
        log_manager=log_manager,
        validator=validator,
        status_checker=status_checker,
        manifest_manager=manifest,
        convert_fastq=True,
        fastq_threads=4,
        max_retries=3,
        batch_size=2,
        s3_handler=None,
        cleanup_local=True,
        pool_cls=ThreadPool
    )

    # --- RUN ORCHESTRATION DRY RUN ---
    orchestrator.process_sra_lists()
    orchestrator.retry_failed()


if __name__ == "__main__":
    main()