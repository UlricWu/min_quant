import pytest
from unittest.mock import patch
from src.dataloader.ftp_downloader import FTPDownloader


@pytest.fixture
def mock_secret():
    with patch("src.dataloader.ftp_downloader.cfg.secret") as mock:
        mock.ftp_host = "localhost"
        mock.ftp_user = "user"
        mock.ftp_password = "pass"
        mock.ftp_port = 21
        yield mock


def test_ftp_download(mock_secret):
    downloader = FTPDownloader()
    assert downloader.host == "localhost"
    assert downloader.user == "user"
