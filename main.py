from config import Config
from log_manager import LogManager
from log_setup import setup_logging
from validators import SRAValidator
from status_checker import DownloadStatusChecker
from sra_downloader import SRADownloader


def main():
    config = Config()
    config.ensure_directories_exist()

    log_manager = LogManager(config.CSV_LOG_DIR, config.PYTHON_LOG_DIR)
    python_log_path = log_manager.generate_python_log()
    setup_logging(python_log_path)

    csv_log_path = log_manager.generate_csv_log()

    validator = SRAValidator(config.SRA_OUTPUT_DIR)
    status_checker = DownloadStatusChecker(config.SRA_OUTPUT_DIR)

    downloader = SRADownloader(
        output_dir=config.SRA_OUTPUT_DIR,
        csv_log_path=csv_log_path,
        log_manager=log_manager,
        validator=validator,
        status_checker=status_checker
    )

    downloader.process_sra_lists()
    downloader.retry_failed()


if __name__ == "__main__":
    main()