import boto3
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class S3Handler:
    def __init__(self, bucket_name: str, prefix: str = ""):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3 = boto3.client("s3")

    def _s3_key(self, filename: str) -> str:
        return f"{self.prefix}/{filename}" if self.prefix else filename

    def upload_file(self, local_path: Path, s3_key: str = None):
        s3_key = s3_key or self._s3_key(local_path.name)
        logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{s3_key}")
        self.s3.upload_file(str(local_path), self.bucket_name, s3_key)

    def file_exists(self, s3_key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise