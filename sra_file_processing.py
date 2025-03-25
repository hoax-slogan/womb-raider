import os
import subprocess
import time
from multiprocessing import Pool
from tqdm import tqdm
import csv
import logging

from config import SRA_OUTPUT_DIR, CSV_LOG_DIR
from utils import get_sra_lists, write_log

# check if .sra, .sralite, .fastq and then log
# properly relog(?) any failed downloads
# AWS functionality
# optional fastq conversion!
# i might want to change the pipline log and csv_log setup


logger = logging.getLogger(__name__)


def download_sra_files(accession, source_file, max_retries=5):
    """
    Download SRA accession files using prefetch, validates succesful downloads 
    via validate_sra_files() + retries and returns/prints a success/fail message
    """
    output_path = os.path.join(SRA_OUTPUT_DIR, accession, f"{accession}.sra")
    source_file = os.path.basename(source_file)

    if os.path.exists(output_path):
        download_status = "Already Exists"
        # revalidate even if it already exists
        validation_status = validate_sra_files(accession)
        logger.info(f"{accession} already exists, skipping download.")
        return [accession, download_status, validation_status, source_file]
    
    logger.info(f"Starting download: {accession}")
    
    for attempt in range(1, max_retries + 1):
        try:
            cmd = ["prefetch", "--max-size", "100G", "-O", SRA_OUTPUT_DIR, accession]
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            if os.path.exists(output_path):
                download_status = "Download OK!"
                # validate after successful download
                validation_status = validate_sra_files(accession)
                return [accession, download_status, validation_status, source_file]
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error downloading {accession}: {e}")
            logger.warning(f"Attempt {attempt}/{max_retries} failed for {accession}, retrying in 5s...")
            time.sleep(5)

    download_status = "Download Failed"
    # pass a v status so log writing doesn't break
    validation_status = validate_sra_files(accession)
    return [accession, download_status, validation_status, source_file]


def validate_sra_files(accession):
    """
    Validates .sra file after downloading using 'vdb-validate'
    """
    sra_file = os.path.join(SRA_OUTPUT_DIR, accession, f"{accession}.sra")

    if not os.path.exists(sra_file):
        logger.warning(f"{accession}.sra not found for validation")
        return "File Missing"

    logger.info(f"Validating {accession}.sra...")
    validation_result = subprocess.run(["vdb-validate", sra_file], capture_output=True, text=True)

    if validation_result.returncode == 0:
        logger.info(f"{accession}: Validation OK!")
        return "Valid"
    else:
        error_details = validation_result.stderr.strip()
        logger.error(f"Validation failed for {accession}: {error_details}")
        return f"Invalid: {error_details}"


def process_sra_list(batch_size=5):
    """
    Reads SRA accesion list, downloads in batches of 5, and logs progress
    """
    for sra_file in get_sra_lists():
        with open(sra_file, "r") as f:
            accessions = [line.strip() for line in f.readlines()]
    
        logger.info(f"Read {len(accessions)} accessions from {sra_file}")
        logger.info(f"First 5 accessions: {accessions[:5]}")

        # multiprocessing to download in parallel
        with Pool(batch_size) as pool:
            results = list(
                tqdm(pool.imap(lambda acc: download_sra_files(acc, sra_file), accessions), total=len(accessions))
            )
        
        write_log(results)
        logger.info(f"Log updated: {CSV_LOG_DIR} ({len(results)} entries added)")


def retry_failed_downloads(batch_size=5):
    with open(CSV_LOG_DIR, "r") as log:
        reader = csv.reader(log)
        next(reader) # Skip header
        failed_downloads = [row[0] for row in reader if "Failed" in row[1] or not "Valid" in row[2]]

    if failed_downloads:
        logger.info(f"Re-attempting failed downloads for: {failed_downloads}")
        
        with Pool(batch_size) as pool:
            results = list(tqdm(pool.imap(download_sra_files, failed_downloads), total=len(failed_downloads)))
        
        write_log(results)
        logger.info(f"Retried failed downloads, log updated: {CSV_LOG_DIR} ({len(results)} entries added)")