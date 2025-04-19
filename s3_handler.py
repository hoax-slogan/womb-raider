import boto3
from pathlib import Path
import logging


logger = logging.getLogger(__name__)


class S3Handler:
    def __init__(self, s3_bucket: str, s3_prefix: str = ""):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3 = boto3.client("s3")


    def _s3_key(self, filename: str) -> str:
        return f"{self.s3_prefix}/{filename}" if self.s3_prefix else filename


    def upload_file(self, local_path: Path, s3_key: str = None):
        s3_key = s3_key or self._s3_key(local_path.name)
        logger.info(f"Uploading {local_path} to s3://{self.s3_bucket}/{s3_key}")
        self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)


    def download_file(self, s3_key: str, local_path: Path):
        logger.info(f"Downloading s3://{self.s3_bucket}/{s3_key} to {local_path}")
        self.s3.download_file(self.s3_bucket, s3_key, str(local_path))


    def file_exists(self, s3_key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise