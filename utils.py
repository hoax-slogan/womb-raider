from pathlib import Path
from typing import Optional
from Bio import Entrez
import xml.etree.ElementTree as ET
import logging
import json


logger = logging.getLogger(__name__)
Entrez.email = "your@email.com"


def get_sra_lists(sra_lists_dir: Path) -> list[Path]:
    return list(sra_lists_dir.glob("*.txt"))

cache_file = Path("srr_gsm_cache.json")

# Load cache if it exists
if cache_file.exists():
    with cache_file.open() as f:
        srr_gsm_cache = json.load(f)
else:
    srr_gsm_cache = {}


def resolve_gsm_from_srr(accession: str) -> Optional[str]:
    """Return GSM ID for given SRR using NCBI Entrez XML parsing (cached)."""
    if accession in srr_gsm_cache:
        return srr_gsm_cache[accession]

    try:
        handle = Entrez.efetch(db="sra", id=accession, rettype="xml", retmode="text")
        root = ET.parse(handle).getroot()
        handle.close()

        for sample_link in root.findall(".//SAMPLE_LINK"):
            for xref in sample_link.findall("XREF_LINK"):
                db = xref.find("DB")
                acc = xref.find("ID")
                if db is not None and db.text == "GEO" and acc is not None and acc.text.startswith("GSM"):
                    gsm = acc.text
                    srr_gsm_cache[accession] = gsm
                    with cache_file.open("w") as f:
                        json.dump(srr_gsm_cache, f, indent=2)
                    return gsm

        logger.warning(f"[utils] No GSM found in SRA XML for {accession}")
        return None

    except Exception as e:
        logger.warning(f"[utils] Failed to fetch GSM for {accession}: {e}")
        return None


def find_gsm_for_demux_pools(accession: str, barcode_dir: Path) -> Optional[Path]:
    """Return .Info.txt path for the pool (GSM) associated with this SRR accession."""
    gsm = resolve_gsm_from_srr(accession)
    if not gsm:
        return None

    matches = list(barcode_dir.glob(f"{gsm}*.Info.txt"))
    if not matches:
        logger.warning(f"[utils] No .Info.txt file found for GSM {gsm}")
        return None

    return matches[0]