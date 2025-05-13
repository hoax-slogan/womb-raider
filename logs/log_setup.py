import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logging(log_path: Path) -> None:
    """
    Configure the root logger to log to both console and file.

    Parameters:
        log_path (Path): Path to the output log file.
    """
    # Clear any existing root handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Console handler (warnings and up)
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)

    # File handler (INFO and up, with rotation)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3)
    file_handler.setLevel(logging.INFO)

    # Unified log config
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[console, file_handler]
    )

    logging.getLogger().info(f"Logging system initialized: {log_path}")