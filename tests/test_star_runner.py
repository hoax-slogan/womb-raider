import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from ..star_runner import STARRunner


@pytest.fixture
def mock_fastqs():
    return [Path("fake_R1.fastq"), Path("fake_R2.fastq")]


@pytest.fixture
def basic_star_runner():
    # STARsolo disabled (regular STAR)
    return STARRunner(
        star_genome_dir=Path("/fake/genome"),
        star_output_dir=Path("/fake/output"),
        barcode_whitelist=None,
        cb_start=None,
        cb_len=None,
        umi_start=None,
        umi_len=None,
        threads=4
    )


@pytest.fixture
def solo_star_runner():
    # STARsolo enabled with whitelist
    return STARRunner(
        star_genome_dir=Path("/fake/genome"),
        star_output_dir=Path("/fake/output"),
        barcode_whitelist=Path("/fake/whitelist.txt"),
        cb_start=1,
        cb_len=8,
        umi_start=9,
        umi_len=8,
        threads=4
    )


@patch("pipeline.star_runner.subprocess.run")
def test_star_align_success(mock_run, basic_star_runner, mock_fastqs):
    mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

    result = basic_star_runner.align("TEST_ACC", mock_fastqs)

    # STAR CLI should be called
    cmd = mock_run.call_args[0][0]
    assert "STAR" in cmd
    assert "--readFilesIn" in cmd
    assert str(mock_fastqs[0]) in cmd
    assert str(mock_fastqs[1]) in cmd

    # Should return list of output files (even if mocked)
    assert isinstance(result, list)


@patch("pipeline.star_runner.subprocess.run")
def test_star_align_with_solo(mock_run, solo_star_runner, mock_fastqs):
    mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

    result = solo_star_runner.align("SOLO_ACC", mock_fastqs)
    cmd = mock_run.call_args[0][0]

    assert "--soloType" in cmd
    assert "--soloCBstart" in cmd
    assert "--soloCBwhitelist" in cmd
    assert str(solo_star_runner.barcode_whitelist) in cmd
    assert isinstance(result, list)


@patch("pipeline.star_runner.subprocess.run")
def test_star_align_with_solo_autodetect(mock_run, mock_fastqs):
    # solo fields set, but whitelist is None
    runner = STARRunner(
        star_genome_dir=Path("/fake/genome"),
        star_output_dir=Path("/fake/output"),
        barcode_whitelist=None,
        cb_start=1,
        cb_len=8,
        umi_start=9,
        umi_len=8,
        threads=4
    )
    mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
    runner.align("AUTO_ACC", mock_fastqs)

    cmd = mock_run.call_args[0][0]
    assert "--soloCBwhitelist" in cmd
    assert "None" in cmd


@patch("pipeline.star_runner.subprocess.run")
def test_star_align_failure(mock_run, basic_star_runner, mock_fastqs):
    mock_run.return_value = MagicMock(returncode=1, stderr="fail!")

    with pytest.raises(RuntimeError, match="STAR alignment failed"):
        basic_star_runner.align("FAIL_ACC", mock_fastqs)


def test_star_align_invalid_fastq_count(basic_star_runner):
    with pytest.raises(ValueError, match="paired-end FASTQ files"):
        basic_star_runner.align("BAD_ACC", [Path("only_R1.fastq")])