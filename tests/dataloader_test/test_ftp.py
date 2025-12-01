#!filepath: tests/dataloader_test/test_ftp_downloader.py
from unittest.mock import patch, MagicMock
from pathlib import Path

import pytest
from src.dataloader.ftp_downloader import FTPDownloader
from src.utils.path import PathManager


@patch("src.dataloader.ftp_downloader.ftplib.FTP")
def test_ftp_download(mock_ftp_class, tmp_path, monkeypatch):
    """
    100% 可通过的 FTPDownloader 测试，包括：
    - 使用 MagicMock 模拟 FTP
    - 正确 mock context manager (__enter__)
    - 模拟 nlst() 返回文件名
    - 模拟 retrbinary() 写出数据
    - 检查 safe_write 是否真正写出文件
    """

    # ------- 准备 fake 环境变量 -------
    monkeypatch.setenv("FTP_HOST", "host")
    monkeypatch.setenv("FTP_USER", "user")
    monkeypatch.setenv("FTP_PASSWORD", "pwd")

    # ------- 修正 PathManager 根目录 -------
    PathManager.set_root(tmp_path)

    # ------- 构造 MagicMock 的 FTP 实例 -------
    mock_ftp = MagicMock()

    # ⭐⭐ 必须让 __enter__ 返回自身，否则 retrbinary 永远不会执行 ⭐⭐
    mock_ftp.__enter__.return_value = mock_ftp

    mock_ftp_class.return_value = mock_ftp

    # ------- mock FTP 文件列表 -------
    mock_ftp.nlst.return_value = ["a.csv", "b.csv"]

    # ------- 模拟 retrbinary 调用 -------
    def fake_retr(cmd, callback):
        callback(b"symbol,price,volume\nAAPL,150.23,1000\n")

    mock_ftp.retrbinary.side_effect = fake_retr

    # ------- 创建 downloader -------
    downloader = FTPDownloader()

    # ------- 执行下载 -------
    downloader.download("2025-08-29")

    # ------- 断言文件确实被写出 -------
    out_dir = tmp_path / "dataloader" / "raw" / "2025-08-29"

    file_a = out_dir / "a.csv"
    file_b = out_dir / "b.csv"

    assert file_a.exists()
    assert file_b.exists()

    # ------- 内容校验 -------
    assert file_a.read_text() == "symbol,price,volume\nAAPL,150.23,1000\n"
