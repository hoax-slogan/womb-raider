from pathlib import Path
from collections import defaultdict
import logging
import pandas as pd
from contextlib import ExitStack


class FastqSplitter:
    def __init__(self, *, fastq_dir: Path, split_fastq_dir: Path, barcode_dir: Path, threads: int = 4):
        self.fastq_dir = fastq_dir
        self.split_fastq_dir = split_fastq_dir
        self.barcode_dir = barcode_dir
        self.threads = threads

        self.read1_path = None
        self.read2_path = None
        self.file_handles = defaultdict(dict)  # {cell_name: {'1': handle, '2': handle}}
        self.unmatched_handle = {}  # {'1': handle, '2': handle}
        self.exit_stack = ExitStack()

        self.logger = logging.getLogger(__name__)
        self.barcode_mapping = self._load_barcode_mapping()


    def _load_barcode_mapping(self) -> dict:
        """Load all .Info.txt files inside the given barcode dataset directory."""
        mapping = {}
        info_files = list(self.barcode_dir.glob("*.txt"))

        if not info_files:
            raise ValueError(f"No .Info.txt files found in {self.barcode_dir}")

        for info_file in info_files:
            df = pd.read_csv(info_file, sep="\t", header=None, names=["Barcode", "CellName"])
            for _, row in df.iterrows():
                barcode = row["Barcode"]
                cell_name = row["CellName"]
                if barcode in mapping:
                    raise ValueError(f"Duplicate barcode detected: {barcode}")
                mapping[barcode] = cell_name

        self.logger.info(f"Loaded {len(mapping)} barcode mappings from {self.barcode_dir}")
        return mapping


    def _open_output_handles(self, cell_name):
        if cell_name not in self.file_handles:
            out1 = self.split_fastq_dir / f"{cell_name}_1.fastq"
            out2 = self.split_fastq_dir / f"{cell_name}_2.fastq"
            self.file_handles[cell_name]['1'] = self.exit_stack.enter_context(open(out1, "w"))
            self.file_handles[cell_name]['2'] = self.exit_stack.enter_context(open(out2, "w"))


    def _open_unmatched_handles(self):
        unmatched1 = self.split_fastq_dir / "unmatched_1.fastq"
        unmatched2 = self.split_fastq_dir / "unmatched_2.fastq"
        self.unmatched_handle['1'] = self.exit_stack.enter_context(open(unmatched1, "w"))
        self.unmatched_handle['2'] = self.exit_stack.enter_context(open(unmatched2, "w"))


    def _close_all_handles(self):
        self.exit_stack.close()


    def _get_pooled_fastqs(self):
        fastqs = list(self.fastq_dir.glob("*.fastq"))
        r1 = [f for f in fastqs if "_1.fastq" in f.name]
        r2 = [f for f in fastqs if "_2.fastq" in f.name]

        if not r1 or not r2:
            raise ValueError("Could not find paired-end pooled FASTQ files in fastq_dir.")

        self.read1_path = r1[0]
        self.read2_path = r2[0]

        self.logger.info(f"Found paired-end FASTQ files: {self.read1_path.name}, {self.read2_path.name}")


    def split_fastqs(self) -> tuple[list[Path], dict]:
        self._get_pooled_fastqs()
        self._open_unmatched_handles()

        unmatched_barcodes = defaultdict(int)
        created_fastqs = set()

        try:
            with open(self.read1_path, "r") as r1_file, open(self.read2_path, "r") as r2_file:
                while True:
                    r1_lines = [r1_file.readline() for _ in range(4)]
                    r2_lines = [r2_file.readline() for _ in range(4)]

                    if not r1_lines[0] or not r2_lines[0]:
                        break  # EOF

                    barcode = r2_lines[1][:8]
                    cell_name = self.barcode_mapping.get(barcode)

                    if cell_name:
                        self._open_output_handles(cell_name)
                        self.file_handles[cell_name]['1'].writelines(r1_lines)
                        self.file_handles[cell_name]['2'].writelines(r2_lines)
                        created_fastqs.add(self.split_fastq_dir / f"{cell_name}_1.fastq")
                        created_fastqs.add(self.split_fastq_dir / f"{cell_name}_2.fastq")
                    else:
                        unmatched_barcodes[barcode] += 1
                        self.unmatched_handle['1'].writelines(r1_lines)
                        self.unmatched_handle['2'].writelines(r2_lines)

        finally:
            self._close_all_handles()

        self.logger.info(f"Splitting completed: {len(created_fastqs)} FASTQ files created.")
        if unmatched_barcodes:
            self.logger.warning(f"Encountered {len(unmatched_barcodes)} unmatched barcodes during splitting.")

        return list(created_fastqs), unmatched_barcodes