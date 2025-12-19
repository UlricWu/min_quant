import pytest

from src.engines.ftp_download_engine import FtpDownloadEngine
def test_resolve_date_valid():
    date = "2025-11-04"
    out = FtpDownloadEngine.resolve_date(date)

    assert out == "2025-11-04"
def test_resolve_date_none_not_allowed():
    with pytest.raises(TypeError):
        FtpDownloadEngine.resolve_date(None)  # type: ignore
@pytest.mark.parametrize(
    "bad_date",
    [
        "20251104",
        "2025/11/04",
        "25-11-04",
        "2025-1-4",
        "",
    ],
)
def test_resolve_date_invalid_format(bad_date):
    with pytest.raises(ValueError):
        FtpDownloadEngine.resolve_date(bad_date)
def test_select_filenames_basic_filter():
    remote = [
        "SH_Trade_20251104.7z",
        "SH_Bond_20251104.7z",
        "",
        "SZ_Order_20251104.7z",
    ]

    selected = FtpDownloadEngine.select_filenames(remote)

    assert selected == [
        "SH_Trade_20251104.7z",
        "SZ_Order_20251104.7z",
    ]
def test_select_filenames_sorted():
    remote = [
        "b_file.7z",
        "a_file.7z",
        "c_file.7z",
    ]

    selected = FtpDownloadEngine.select_filenames(remote)

    assert selected == [
        "a_file.7z",
        "b_file.7z",
        "c_file.7z",
    ]
def test_select_filenames_type_check():
    with pytest.raises(TypeError):
        FtpDownloadEngine.select_filenames("not-a-list")  # type: ignore
def test_select_filenames_deterministic():
    remote = [
        "x.7z",
        "y.7z",
        "Bond.7z",
    ]

    out1 = FtpDownloadEngine.select_filenames(remote)
    out2 = FtpDownloadEngine.select_filenames(remote)

    assert out1 == out2
