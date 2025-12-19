from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.engines.ftp_download_engine import FtpDownloadEngine
from src.adapters.ftp_download_adapter import (
    FtpDownloadAdapter,
    DownloadBackend,
)
from src.config.secret_config import SecretConfig


@patch("ftplib.FTP")
def test_adapter_download_ftplib(mock_ftp, tmp_path):
    # ---- mock FTP instance ----
    ftp = MagicMock()
    mock_ftp.return_value = ftp

    ftp.nlst.return_value = ["A.7z", "Bond_X.7z"]
    ftp.pwd.return_value = "/level2/2025-01-01"

    # ---- config ----
    secret = SecretConfig(
        ftp_host="127.0.0.1",
        ftp_port=21,
        ftp_user="user",
        ftp_password="pwd",
    )

    engine = FtpDownloadEngine()

    adapter = FtpDownloadAdapter(
        secret=secret,
        backend=DownloadBackend.FTPLIB,
        engine=engine,
        inst=None,
    )

    # ---- execute ----
    adapter.download_date("2025-01-01", tmp_path)

    # ---- assertions ----
    # 核心断言（稳）
    ftp.connect.assert_called_once()
    ftp.login.assert_called_once()
    ftp.retrbinary.assert_called_once()

    # 可选：只断言 date 目录
    cwd_args = [c.args[0] for c in ftp.cwd.call_args_list]
    assert "2025-01-01" in cwd_args

    # Bond 文件应被过滤，只下载一个
    ftp.retrbinary.assert_called_once()

from pathlib import Path
from unittest.mock import patch, MagicMock

from src.engines.ftp_download_engine import FtpDownloadEngine
from src.adapters.ftp_download_adapter import (
    FtpDownloadAdapter,
    DownloadBackend,
)
from src.config.secret_config import SecretConfig


@patch("subprocess.run")
@patch("ftplib.FTP")
def test_adapter_download_curl(mock_ftp, mock_run, tmp_path):
    ftp = MagicMock()
    mock_ftp.return_value = ftp

    ftp.nlst.return_value = ["A.7z"]
    ftp.pwd.return_value = "/level2/2025-01-01"

    mock_run.return_value.returncode = 0
    mock_run.return_value.stderr = ""

    secret = SecretConfig(
        ftp_host="127.0.0.1",
        ftp_port=20240,
        ftp_user="user",
        ftp_password="pwd",
    )

    engine = FtpDownloadEngine()

    adapter = FtpDownloadAdapter(
        secret=secret,
        backend=DownloadBackend.CURL,
        engine=engine,
        inst=None,
    )

    adapter.download_date("2025-01-01", tmp_path)

    # curl subprocess 必须被调用
    mock_run.assert_called_once()
