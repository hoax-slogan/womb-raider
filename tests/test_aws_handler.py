import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from ..s3_handler import S3Handler 


@pytest.fixture
def mock_boto3_client():
    with patch("pipeline.s3_handler.boto3.client") as mock_client:
        yield mock_client


def test_upload_file_calls_boto3_upload(mock_boto3_client):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    handler = S3Handler(s3_bucket="test-bucket")
    local_path = Path("fake_file.txt")

    handler.upload_file(local_path)

    mock_s3.upload_file.assert_called_once_with(
        str(local_path), "test-bucket", "fake_file.txt"
    )


def test_file_exists_true(mock_boto3_client):
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {}
    mock_boto3_client.return_value = mock_s3

    handler = S3Handler(s3_bucket="test-bucket")

    assert handler.file_exists("exists.txt") is True
    mock_s3.head_object.assert_called_once_with(
        Bucket="test-bucket", Key="exists.txt"
    )


def test_file_exists_false(mock_boto3_client):
    mock_s3 = MagicMock()
    error = mock_s3.exceptions.ClientError = type(
        "ClientError", (Exception,), {"response": {"Error": {"Code": "404"}}}
    )
    mock_s3.head_object.side_effect = error()
    mock_boto3_client.return_value = mock_s3

    handler = S3Handler(s3_bucket="test-bucket")

    assert handler.file_exists("missing.txt") is False


def test_download_file_calls_boto3_download(mock_boto3_client):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    handler = S3Handler(s3_bucket="test-bucket")
    local_path = Path("downloaded_file.txt")

    handler.download_file("remote_key.txt", local_path)

    mock_s3.download_file.assert_called_once_with(
        "test-bucket", "remote_key.txt", str(local_path)
    )