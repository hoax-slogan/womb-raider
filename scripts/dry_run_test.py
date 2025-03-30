from pathlib import Path
import subprocess
from pipeline.orchestrator import SRAOrchestrator
from pipeline.validators import SRAValidator
from pipeline.log_manager import LogManager
from pipeline.status_checker import DownloadStatusChecker
from pipeline.config import Config
from pipeline.log_setup import setup_logging


class FakeSubprocessResult:
    def __init__(self):
        self.returncode = 0
        self.stderr = ""


def fake_subprocess_run(*args, **kwargs):
    return FakeSubprocessResult()


def main():
    # Setup config and logging
    cfg = Config()
    cfg.ensure_directories_exist()
    setup_logging(cfg.PYTHON_LOG_DIR / "dry_run.log")

    # Create fake SRA list file
    fake_list = cfg.SRA_LISTS_DIR / "fake_list.txt"
    fake_list.write_text("SRR_FAKE001\nSRR_FAKE002\n")

    # Simulate .sra file for one accession
    fake_existing = cfg.SRA_OUTPUT_DIR / "SRR_FAKE001"
    fake_existing.mkdir(parents=True, exist_ok=True)
    (fake_existing / "SRR_FAKE001.sra").touch()

    # Patch subprocess and methods for dry simulation
    subprocess.run = fake_subprocess_run
    SRAValidator.validate = lambda self, acc: "Valid"
    DownloadStatusChecker.check_status = lambda self, acc: "Already Exists" if acc == "SRR_FAKE001" else "Not Found"
    DownloadStatusChecker.confirm_download = lambda self, acc: "Download OK!"

    # Instantiate pipeline components
    log_manager = LogManager(csv_log_dir=cfg.CSV_LOG_DIR, python_log_dir=cfg.PYTHON_LOG_DIR)
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
        convert_fastq=False,
        batch_size=2,
        max_retries=3
    )

    orchestrator.process_sra_lists()


if __name__ == "__main__":
    main()