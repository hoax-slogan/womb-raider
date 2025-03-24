import os
import yaml


SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))

# load yaml config
with open(os.path.join(SCRIPT_DIR, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

# Base data directory
DATA_DIR = os.path.join(BASE_DIR, config["data_dir"])

# Subdirectories
SRA_LISTS_DIR = os.path.join(DATA_DIR, config["subdirs"]["lists"])
SRA_OUTPUT_DIR = os.path.join(DATA_DIR, config["subdirs"]["output"])
LOGS_DIR = os.path.join(DATA_DIR, config["subdirs"]["logs"])
FASTQ_DIR = os.path.join(DATA_DIR, config["subdirs"]["fastq"])

# Log files
CSV_LOG = os.path.join(LOGS_DIR, config["logs"]["csv"])
LOGGER_LOG = os.path.join(LOGS_DIR, config["logs"]["log"])

