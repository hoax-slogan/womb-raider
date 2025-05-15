import logging
import json
from pathlib import Path
from typing import Optional
from Bio import Entrez
import xml.etree.ElementTree as ET


class GSMResolver:
    def __init__(self, *, gsm_cache_path: Path, barcode_dir: Path, entrez_email: str):
        self.logger = logging.getLogger(__name__)
        self.cache_path = gsm_cache_path
        self.barcode_dir = barcode_dir
        self._cache = self._load_cache()

        Entrez.email = entrez_email  

        self.logger.debug(f"GSMResolver initialized with cache: {self.cache_path}, barcodes: {self.barcode_dir}")


    def _load_cache(self) -> dict:
        if self.cache_path.exists():
            try:
                with self.cache_path.open("r") as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to read GSM cache file {self.cache_path}: {e}")
        return {}


    def _save_cache(self):
        try:
            with self.cache_path.open("w") as f:
                json.dump(self._cache, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Failed to save GSM cache to {self.cache_path}: {e}")


    def resolve(self, accession: str) -> Optional[str]:
        """Resolve GSM ID for a given SRR accession via NCBI Entrez or cache."""
        if accession in self._cache:
            return self._cache[accession]

        self.logger.info(f"{accession} not found in GSM cache — querying Entrez.")

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
                        self._cache[accession] = gsm
                        self._save_cache()
                        return gsm

            self.logger.warning(f"No GSM found in Entrez XML for {accession}")
            return None

        except Exception as e:
            self.logger.warning(f"Entrez fetch failed for {accession}: {e}")
            return None


    def find_info_file(self, accession: str) -> Optional[Path]:
        """Return path to the .Info.txt barcode pool for the accession."""
        gsm = self.resolve(accession)
        if not gsm:
            return None

        matches = list(self.barcode_dir.glob(f"{gsm}*.Info.txt"))
        if not matches:
            self.logger.warning(f"No .Info.txt file found for GSM {gsm}")
            return None

        return matches[0]