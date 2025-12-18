import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from src.engines.ftp_download_engine import FtpDownloadEngine
from src.adapters.ftp_download_adapter import FtpDownloadAdapter
from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.context import PipelineContext


def test_engine_resolve_date():
    eng = FtpDownloadEngine()
    assert eng.resolve_date("2025-01-01") == "2025-01-01"


def test_engine_filter_filenames():
    eng = FtpDownloadEngine()
    files = ["A.csv", "Bond_X.csv", "", None, "SH_Trade.csv"]
    res = eng.filter_filenames(files)
    assert res == ["A.csv", "SH_Trade.csv"]


@patch("ftplib.FTP")
@patch("src.utils.filesystem.FileSystem.safe_write")
def test_adapter_download(mock_write, mock_ftp):
    # mock FTP
    ftp = MagicMock()
    mock_ftp.return_value = ftp
    ftp.nlst.return_value = ["A.7z", "Bond_X.7z"]

    # adapter
    eng = FtpDownloadEngine()
    adapter = FtpDownloadAdapter(
        host="host",
        user="user",
        password="pwd",
        port=21,
        remote_root="level2",
        engine=eng,
    )

    tmp = Path("/tmp/test_download")
    adapter.download_date("2025-01-01", tmp)

    ftp.connect.assert_called_once()
    ftp.login.assert_called_once()
    ftp.cwd.assert_any_call("level2")
    ftp.cwd.assert_any_call("2025-01-01")

    # Should download only A.7z (Bond filtered out)
    ftp.retrbinary.assert_called_once()


def test_download_step_invokes_adapter():
    eng = FtpDownloadEngine()
    adapter = MagicMock()
    step = DownloadStep(adapter)
    ctx = PipelineContext(
        date="2025-01-01",
        raw_dir=Path("/tmp/raw"),
        parquet_dir=Path("/tmp/pq"),
        symbol_dir=Path("/tmp/sym"),
    )

    step.run(ctx)
    adapter.download_date.assert_called_once_with("2025-01-01", Path("/tmp/raw"))
