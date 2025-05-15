from pathlib import Path
import logging


logger = logging.getLogger(__name__)


def get_sra_lists(sra_lists_dir: Path) -> list[Path]:
    return list(sra_lists_dir.glob("*.txt"))
