from pathlib import Path
from collections import defaultdict
import pandas as pd


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

        return mapping


    def _open_output_handles(self, cell_name):
        if cell_name not in self.file_handles:
            out1 = self.split_fastq_dir / f"{cell_name}_1.fastq"
            out2 = self.split_fastq_dir / f"{cell_name}_2.fastq"
            self.file_handles[cell_name]['1'] = open(out1, "w")
            self.file_handles[cell_name]['2'] = open(out2, "w")


    def _open_unmatched_handles(self):
        unmatched1 = self.split_fastq_dir / "unmatched_1.fastq"
        unmatched2 = self.split_fastq_dir / "unmatched_2.fastq"
        self.unmatched_handle['1'] = open(unmatched1, "w")
        self.unmatched_handle['2'] = open(unmatched2, "w")


    def _close_all_handles(self):
        for handles in self.file_handles.values():
            handles['1'].close()
            handles['2'].close()
        if self.unmatched_handle:
            self.unmatched_handle['1'].close()
            self.unmatched_handle['2'].close()


    def _get_pooled_fastqs(self):
        fastqs = list(self.fastq_dir.glob("*.fastq"))
        r1 = [f for f in fastqs if "_1.fastq" in f.name]
        r2 = [f for f in fastqs if "_2.fastq" in f.name]

        if not r1 or not r2:
            raise ValueError("Could not find paired-end pooled FASTQ files in fastq_dir.")

        self.read1_path = r1[0]
        self.read2_path = r2[0]


    def split_fastqs(self):
        self._get_pooled_fastqs()
        self._open_unmatched_handles()

        unmatched_barcodes = defaultdict(int)

        with open(self.read1_path, "r") as r1_file, open(self.read2_path, "r") as r2_file:
            while True:
                r1_lines = [r1_file.readline() for _ in range(4)]
                r2_lines = [r2_file.readline() for _ in range(4)]

                if not r1_lines[0] or not r2_lines[0]:
                    break  # EOF

                barcode = r2_lines[1][:8]  # first 8 bp
                cell_name = self.barcode_mapping.get(barcode)

                if cell_name:
                    self._open_output_handles(cell_name)
                    self.file_handles[cell_name]['1'].writelines(r1_lines)
                    self.file_handles[cell_name]['2'].writelines(r2_lines)
                else:
                    unmatched_barcodes[barcode] += 1
                    self.unmatched_handle['1'].writelines(r1_lines)
                    self.unmatched_handle['2'].writelines(r2_lines)

        self._close_all_handles()
        print(f"Splitting completed. Outputs saved to {self.split_fastq_dir}")
        self._write_unmatched_summary(unmatched_barcodes)


    def _write_unmatched_summary(self, unmatched_barcodes: dict):
        summary_path = self.split_fastq_dir / "unmatched_barcodes.txt"
        with open(summary_path, "w") as f:
            f.write("Barcode\tCount\n")
            for barcode, count in unmatched_barcodes.items():
                f.write(f"{barcode}\t{count}\n")
        
        print(f"Unmatched barcode summary saved to {summary_path}")