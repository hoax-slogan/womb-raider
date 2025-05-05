from pathlib import Path
from collections import defaultdict
import logging
import pandas as pd
from contextlib import ExitStack

from .utils import find_gsm_for_demux_pools


class FASTQSplitter:
    def __init__(self, *, accession: str, fastq_dir: Path, split_fastq_dir: Path, barcode_dir: Path, threads: int = 4):
        self.accession = accession
        self.fastq_dir = fastq_dir
        self.split_fastq_dir = split_fastq_dir
        self.barcode_dir = barcode_dir
        self.threads = threads

        self.read1_path = None
        self.read2_path = None
        self.barcode_length = None  # inferred from info.txt

        self.file_handles = defaultdict(dict)  # {sample_name: {'1': handle, '2': handle}}
        self.unmatched_handle = {} # {'1': handle, '2': handle}
        self.exit_stack = ExitStack()

        self.logger = logging.getLogger(__name__)
        self.barcode_mapping = self._load_barcode_mapping()


    def _load_barcode_mapping(self) -> dict:
        mapping = {}  # {gsm_id: {barcode: sample_name}}

        info_files = list(self.barcode_dir.glob("*.Info.txt"))
        if not info_files:
            raise ValueError(f"No .Info.txt files found in {self.barcode_dir}")

        for info_file in info_files:
            gsm_id = info_file.stem.split("_")[0]  # e.g. GSM3239712
            df = pd.read_csv(info_file, sep="\t", header=None, names=["Barcode", "SampleName"])

            pool_mapping = {}
            for _, row in df.iterrows():
                barcode = row["Barcode"]
                sample_name = row["SampleName"]

                if barcode in pool_mapping:
                    raise ValueError(f"Duplicate barcode {barcode} in {info_file}")
                pool_mapping[barcode] = sample_name

            mapping[gsm_id] = pool_mapping

        return mapping


    def _open_output_handles(self, sample_name):
        if sample_name not in self.file_handles:
            out1 = self.split_fastq_dir / f"{sample_name}_1.fastq"
            out2 = self.split_fastq_dir / f"{sample_name}_2.fastq"
            self.file_handles[sample_name]['1'] = self.exit_stack.enter_context(open(out1, "w"))
            self.file_handles[sample_name]['2'] = self.exit_stack.enter_context(open(out2, "w"))


    def _open_unmatched_handles(self):
        unmatched1 = self.split_fastq_dir / f"{self.accession}_unmatched_1.fastq"
        unmatched2 = self.split_fastq_dir / f"{self.accession}_unmatched_2.fastq"
        self.unmatched_handle['1'] = self.exit_stack.enter_context(open(unmatched1, "w"))
        self.unmatched_handle['2'] = self.exit_stack.enter_context(open(unmatched2, "w"))


    def _close_all_handles(self):
        self.exit_stack.close()


    def _get_pooled_fastqs(self):
        r1 = self.fastq_dir / f"{self.accession}_1.fastq"
        r2 = self.fastq_dir / f"{self.accession}_2.fastq"

        if not r1.exists() or not r2.exists():
            raise FileNotFoundError(f"Could not find paired FASTQ files for accession {self.accession} in {self.fastq_dir}")

        self.read1_path = r1
        self.read2_path = r2
        self.logger.info(f"Found paired-end FASTQ files: {r1.name}, {r2.name}")


    def split_fastqs(self) -> tuple[list[Path], dict]:
        self._get_pooled_fastqs()
        self._open_unmatched_handles()

        # identify the correct barcode pool
        info_path = find_gsm_for_demux_pools(self.accession, self.barcode_dir)
        if not info_path:
            raise RuntimeError(f"Could not resolve pool (GSM) for SRR {self.accession}")

        gsm_id = info_path.stem.split("_")[0]
        pool_barcodes = self.barcode_mapping.get(gsm_id)
        if not pool_barcodes:
            raise ValueError(f"No barcode mapping found for GSM {gsm_id}")

        # infer barcode length from this pool
        lengths = {len(bc) for bc in pool_barcodes}
        if len(lengths) > 1:
            raise ValueError(f"Inconsistent barcode lengths in pool {gsm_id}: {lengths}")
        self.barcode_length = lengths.pop()

        unmatched_barcodes = defaultdict(int)
        reads_per_cell = defaultdict(int)
        created_fastqs = set()

        total_reads = 0
        matched_reads = 0

        try:
            with open(self.read1_path, "r") as r1_file, open(self.read2_path, "r") as r2_file:
                while True:
                    r1_lines = [r1_file.readline() for _ in range(4)]
                    r2_lines = [r2_file.readline() for _ in range(4)]

                    if not r1_lines[0] or not r2_lines[0]:
                        break  # EOF

                    total_reads += 1
                    barcode = r2_lines[1][:self.barcode_length]
                    sample_name = pool_barcodes.get(barcode)

                    if sample_name:
                        self._open_output_handles(sample_name)
                        self.file_handles[sample_name]['1'].writelines(r1_lines)
                        self.file_handles[sample_name]['2'].writelines(r2_lines)
                        reads_per_cell[sample_name] += 1
                        matched_reads += 1

                        created_fastqs.add(self.split_fastq_dir / f"{sample_name}_1.fastq")
                        created_fastqs.add(self.split_fastq_dir / f"{sample_name}_2.fastq")
                    else:
                        unmatched_barcodes[barcode] += 1
                        self.unmatched_handle['1'].writelines(r1_lines)
                        self.unmatched_handle['2'].writelines(r2_lines)

        finally:
            self._close_all_handles()

        unmatched_reads = total_reads - matched_reads
        unmatched_rate = round((unmatched_reads / total_reads) * 100, 2) if total_reads else 0.0

        self.logger.info(f"[{self.accession}] Splitting completed: {len(created_fastqs)} FASTQ files created.")
        self.logger.info(f"[{self.accession}] Total reads: {total_reads} | Matched: {matched_reads} | Unmatched: {unmatched_reads} ({unmatched_rate}%)")

        if matched_reads == 0:
            self.logger.warning(f"[{self.accession}] No barcodes matched — this sample may not be multiplexed.")
        if unmatched_barcodes:
            self.logger.warning(f"[{self.accession}] {len(unmatched_barcodes)} unique unmatched barcodes found.")

        summary = {
            "accession": self.accession,
            "total_reads": total_reads,
            "matched_reads": matched_reads,
            "unmatched_reads": unmatched_reads,
            "unmatched_rate": unmatched_rate,
            "unique_barcodes_matched": len(reads_per_cell),
            "reads_per_cell": dict(reads_per_cell),
            "top_unmatched_barcodes": dict(sorted(unmatched_barcodes.items(), key=lambda x: -x[1])[:10]),
        }

        return list(created_fastqs), summary