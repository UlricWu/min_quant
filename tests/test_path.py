#!filepath: tests/test_path.py
import pytest
from pathlib import Path

from src import path


def test_root_detected():
    """测试自动获取根路径正确"""
    root = path.root()
    assert root.exists()
    assert (root / "src").exists()


def test_set_root(tmp_path):
    """测试测试环境可手动设置根路径"""
    new_root = tmp_path / "project"
    new_root.mkdir()

    path.set_root(new_root)

    assert path.root() == new_root
    assert path.data_dir() == new_root / "data"
    assert path.logs_dir() == new_root / "logs"


def test_directory_functions(tmp_path):
    """测试常用目录路径组合"""
    path.set_root(tmp_path)

    assert path.raw_dir() == tmp_path / "data" / "raw"
    assert path.raw_csv_dir() == tmp_path / "data" / "raw_csv"
    assert path.parquet_dir() == tmp_path / "data" / "parquet"
    assert path.logs_dir() == tmp_path / "logs"
    assert path.models_dir() == tmp_path / "models"


def test_config_file_path(tmp_path):
    """测试 config_file 拼接是否正确"""
    path.set_root(tmp_path)

    cfg = path.config_file("base.yaml")
    assert cfg == tmp_path / "config" / "base.yaml"
