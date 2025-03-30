import argparse


class CLIArgs:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Womb Raider Pipeline CLI")
        self._add_arguments()
        self.args = self.parser.parse_args()


    def _add_arguments(self):
        self.parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simulate the pipeline without executing downloads or conversions",
        )
        
        self.parser.add_argument(
            "--convert-fastq",
            action="store_true",
            help="Convert .sra to .fastq after download",
        )

        self.parser.add_argument(
            "--batch-size",
            type=int,
            default=5,
            help="Number of parallel downloads",
        )

        self.parser.add_argument(
            "--max-retries",
            type=int,
            default=5,
            help="Maximum retries for failed downloads",
        )

        self.parser.add_argument(
            "--use-s3",
            action="store_true",
            help="Enable S3 upload after download",
        )

    def get_args(self):
        return self.args