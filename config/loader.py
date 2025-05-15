import os
import yaml
import math
from pathlib import Path
from dotenv import load_dotenv

from .schema import *


load_dotenv()


def _resolve_threads(raw_threads: str | int | None) -> int:
    """
    Resolves number of threads to use based on # of cpu cores available on
    the user's machine. Supports int, "auto", "X%", or None.
    """
    available = os.cpu_count() or 1
    
    # if none, use 75% of available cores
    if raw_threads is None:
        resolved = max(1, math.floor(available * 0.75))
        print(f"[config] No thread setting provided. Using 75% of cores: {resolved}/{available}")
        return resolved

    # if int, use that number
    if isinstance(raw_threads, int):
        print(f"[config] Using explicitly specified thread count: {raw_threads}")
        return raw_threads

    # if cli arg "auto" use all available cores
    if isinstance(raw_threads, str):
        if raw_threads == "auto":
            print(f"[config] Thread setting '{raw_threads}' → using all cores: {available}")
            return available
        
        # elif cli arg ends with % use core percentage
        elif raw_threads.endswith("%"):
            stripped = raw_threads[:-1]
            if "%" in stripped:
                raise ValueError(f"Too many '%' signs in thread setting: {raw_threads}")
            try:
                pct = float(stripped) / 100
                resolved = max(1, math.floor(available * pct))
                print(f"[config] Thread setting '{raw_threads}' → using {resolved}/{available} cores")
                return resolved
            except ValueError:
                raise ValueError(f"Invalid thread percentage: {raw_threads}")
            
        try:
            resolved = int(raw_threads)
            print(f"[config] Thread setting '{raw_threads}' coerced to integer: {resolved}")
            return resolved
        except ValueError:
            raise ValueError(f"Invalid thread value: {raw_threads}")

    raise ValueError(f"Unrecognized thread setting: {raw_threads}")


def load_config(config_path: Path) -> WombRaiderConfig:
    with open(config_path, 'r') as f:
        raw = yaml.safe_load(f)

    return WombRaiderConfig(
        data_dir=raw["data_dir"],
        subdirs=SubdirsConfig(**raw["subdirs"]),
        logs=LogsConfig(**raw["logs"]),
        star=StarConfig(**raw["star"]),
        demux_runtime=DemuxRuntimeConfig(**raw["demux_runtime"]),
        batch_size=raw.get("batch_size", 5),
        threads=_resolve_threads(raw.get("threads")),
        max_retries=raw.get("max_retries", 5),
        s3_bucket=raw.get("s3_bucket", None),
        s3_prefix=raw.get("s3_prefix", "")
    )


def get_database_url() -> str:
    url = os.getenv("database_url")
    if not url:
        raise ValueError("Missing database_url in environment. Please check your .env file.")
    return url