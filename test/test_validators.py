from unittest.mock import patch, MagicMock
import pytest
from ..validators import SRAValidator


@pytest.fixture
def validator(tmp_path):
    return SRAValidator(output_dir=tmp_path)


def test_validate_file_missing(validator):
    accession = "SRR123456"
    result = validator.validate(accession)
    assert result == "File Missing"


@patch("pipeline.validators.subprocess.run")
@patch("pipeline.validators.Path.exists")
def test_validate_success(mock_exists, mock_run, validator):
    accession = "SRR123456"
    mock_exists.return_value = True
    mock_run.return_value = MagicMock(returncode=0, stderr="")
    result = validator.validate(accession)
    assert result == "Valid"


@patch("pipeline.validators.subprocess.run")
@patch("pipeline.validators.Path.exists")
def test_validate_failure(mock_exists, mock_run, validator):
    accession = "SRR123456"
    mock_exists.return_value = True
    mock_run.return_value = MagicMock(returncode=1, stderr="Validation error")
    result = validator.validate(accession)
    assert result.startswith("Invalid:")
    assert "Validation error" in result