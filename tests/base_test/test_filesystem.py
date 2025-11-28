#!filepath: tests/test_filesystem.py
import os
import pytest
from pathlib import Path

from src.utils.filesystem import FileSystem
from src import logs


def test_ensure_dir(tmp_path):
    """测试 ensure_dir 是否能正确创建目录"""
    new_dir = tmp_path / "new_folder"
    assert not new_dir.exists()

    FileSystem.ensure_dir(new_dir)
    assert new_dir.exists()
    assert new_dir.is_dir()


def test_file_exists(tmp_path):
    """测试 file_exists"""
    f = tmp_path / "file.txt"
    f.write_text("hello")

    assert FileSystem.file_exists(f) is True
    assert FileSystem.file_exists(tmp_path / "not_exist.txt") is False


def test_safe_write(tmp_path):
    """测试 safe_write 是否原子写入并不残留 tmp 文件"""
    file_path = tmp_path / "data.bin"

    data = b"1234567890"
    FileSystem.safe_write(file_path, data)

    # 原子写入结果必须存在
    assert file_path.exists()
    assert file_path.read_bytes() == data

    # 临时文件不应残留
    tmp_file = file_path.with_suffix(".tmp")
    assert not tmp_file.exists()


def test_scan_dir(tmp_path):
    """测试 scan_dir 能正确扫描并按名称排序"""
    f1 = tmp_path / "a.csv"
    f2 = tmp_path / "b.csv"
    f1.write_text("1")
    f2.write_text("2")

    files = FileSystem.scan_dir(tmp_path, suffix=".csv")

    assert files == [f1, f2]


def test_get_file_size(tmp_path):
    """测试 get_file_size 返回正确大小"""
    f = tmp_path / "file.bin"
    data = b"hello world"
    f.write_bytes(data)

    size = FileSystem.get_file_size(f)
    assert size == len(data)


def test_get_dir_size(tmp_path):
    """测试 get_dir_size 返回目录总大小"""
    d = tmp_path / "data"
    d.mkdir()

    (d / "a.bin").write_bytes(b"12345")
    (d / "b.bin").write_bytes(b"123")

    total = FileSystem.get_dir_size(d)
    assert total == 5 + 3


def test_clean_temp_files(tmp_path):
    """测试清理临时文件功能"""
    d = tmp_path / "tmpdata"
    d.mkdir()

    tmp1 = d / "x.tmp"
    tmp2 = d / "y.tmp"
    tmp1.write_text("1")
    tmp2.write_text("2")

    count = FileSystem.clean_temp_files(d)

    assert count == 2
    assert not tmp1.exists()
    assert not tmp2.exists()


def test_remove_file(tmp_path):
    """测试 remove 可以删除文件"""
    f = tmp_path / "file.txt"
    f.write_text("hello")

    assert f.exists()
    FileSystem.remove(f)
    assert not f.exists()


def test_remove_directory(tmp_path):
    """测试 remove 可以删除目录"""
    d = tmp_path / "folder"
    d.mkdir()
    (d / "a.txt").write_text("test")

    FileSystem.remove(d)
    assert not d.exists()
