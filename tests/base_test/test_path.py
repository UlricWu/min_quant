#!filepath: tests/test_path_manager.py
import pytest
from pathlib import Path

from src.utils.path import PathManager


def test_detect_root_structure(tmp_path):
    """
    模拟服务器目录结构：
        tmp/
         ├── dev/
         │     └── code/
         │           └── src/utils/path.py (模拟)
         ├── data/
         └── shared/
    """

    dev = tmp_path / "dev"
    code = dev / "code"
    src = code / "src" / "utils"

    src.mkdir(parents=True)
    (tmp_path / "data").mkdir()
    (tmp_path / "shared").mkdir()

    # mock __file__ 所在目录
    fake_file = src / "path.py"

    PathManager.set_root(None)  # 重置根目录

    # monkeypatch Path(__file__) → fake_file
    PathManager._root = None
    PathManager.detect_root = classmethod(lambda cls: dev)

    assert PathManager.root() == dev


def test_set_root_overrides(tmp_path):
    """测试手动设置 root"""
    new_root = tmp_path / "project"
    new_root.mkdir()

    PathManager.set_root(new_root)

    assert PathManager.root() == new_root


def test_data_and_shared_dirs(tmp_path):
    """
    root = tmp/dev
    data = root.parent / data
    shared = root.parent / shared
    """
    dev = tmp_path / "dev"
    dev.mkdir()

    (tmp_path / "data").mkdir()
    (tmp_path / "shared").mkdir()

    PathManager.set_root(dev)

    assert PathManager.data_dir() == tmp_path / "data"
    assert PathManager.shared_dir() == tmp_path / "shared"


def test_symbol_and_parquet_dirs(tmp_path):
    dev = tmp_path / "dev"
    dev.mkdir()
    (tmp_path / "data").mkdir()

    PathManager.set_root(dev)

    # test symbol
    assert PathManager.symbol_dir("600000") == tmp_path / "data" / "symbol" / "600000"
    assert PathManager.order_dir("600000", "20250103") == \
           tmp_path / "data" / "symbol" / "600000" / "20250103" / "Order.parquet"


def test_config_resolution(tmp_path):
    """
    config_file(name) 查找顺序:
        1. src/config/name
        2. shared/configs/name
    """

    # 构造目录结构
    dev = tmp_path / "dev"
    code_src = dev / "code" / "src" / "config"
    shared_cfg = tmp_path / "shared" / "configs"

    code_src.mkdir(parents=True)
    shared_cfg.mkdir(parents=True)

    # 文件分别放两个位置
    file1 = code_src / "a.yaml"
    file1.write_text("project config")

    file2 = shared_cfg / "b.yaml"
    file2.write_text("shared config")

    PathManager.set_root(dev)
    # print(PathManager.project_dir())

    # # 1) 项目内优先级
    assert PathManager.config_file("a.yaml") == file1
    #
    # 2) 不在项目内则 fallback 到 shared/configs
    assert PathManager.config_file("b.yaml") == file2

    # 3) 都不存在 → 返回 shared/configs/name（但文件不存在）
    expect = shared_cfg / "c.yaml"
    assert PathManager.config_file("c.yaml") == expect
