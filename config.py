import os


SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
SRA_DIR = os.path.join(BASE_DIR, "sra_data")

# Subdirectories
SRA_LISTS_DIR = os.path.join(SRA_DIR, "sra_lists")
SRA_OUTPUT_DIR = os.path.join(SRA_DIR, "sra_files") #where sra list downloads live
LOGS_DIR = os.path.join(SRA_DIR, "logs")
FASTQ_DIR = os.path.join(SRA_DIR, "fastq_files")

# Logs
CSV_LOGS = os.path.join(LOGS_DIR, "progress.csv")
LOG_FILES = os.path.join(LOGS_DIR, "pipeline.log")

