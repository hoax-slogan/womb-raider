import logging
import pytest
from scripts.log_setup import setup_logging


def test_setup_logging_creates_log_file_and_logs(tmp_path):
    log_path = tmp_path / "test_pipeline.log"
    setup_logging(log_path)

    logger = logging.getLogger()
    logger.info("This is a test log message.")

    assert log_path.exists()

    with log_path.open("r") as f:
        contents = f.read()

    assert "This is a test log message." in contents
    assert "Logging system initialized" in contents