#!filepath: src/utils/path.py
from pathlib import Path
from typing import Optional

from pydantic.v1.datetime_parse import date_re

from src import logs


class PathManager:
    """
    统一管理项目中所有路径。
    默认情况下自动从当前文件往上寻找项目根目录（包含 src/ 的目录）。
    可通过 set_root() 在测试中覆盖根路径。
    """

    _root: Optional[Path] = None

    @classmethod
    def detect_root(cls) -> Path:
        """
        自动检测项目根路径：
        原理：从当前文件开始向上查找包含 src/ 目录的路径。
        """
        current = Path(__file__).resolve()

        for parent in current.parents:
            if (parent / "src").exists():
                logs.debug(f"[PathManager] 检测到项目根目录: {parent}")
                return parent

        # 如果未检测到，fallback 为当前目录
        logs.warning("[PathManager] 未检测到项目根目录，使用当前路径")
        return Path.cwd()

    @classmethod
    def root(cls) -> Path:
        """返回项目根路径，如果未设定，则自动检测"""
        if cls._root is None:
            cls._root = cls.detect_root()
        return cls._root

    @classmethod
    def set_root(cls, new_root: Path | str):
        """
        用于测试环境或自定义环境。
        """
        cls._root = Path(new_root)
        logs.debug(f"[PathManager] 根目录已更新为: {cls._root}")

    # =========================
    #  通用目录
    # =========================
    @classmethod
    def data_dir(cls) -> Path:
        return cls.root() / "data"

    @classmethod
    def raw_dir(cls, date=None) -> Path:
        p = cls.data_dir() / "raw"
        if date:
            return p / date
        return p

    @classmethod
    def raw_csv_dir(cls) -> Path:
        return cls.data_dir() / "raw_csv"

    @classmethod
    def parquet_dir(cls, date=None) -> Path:
        p = cls.data_dir() / "parquet"
        return p

    @classmethod
    def logs_dir(cls) -> Path:
        return cls.root() / "logs"

    @classmethod
    def config_dir(cls) -> Path:
        return cls.root() / "config"

    @classmethod
    def config_file(cls, name: str) -> Path:
        return cls.config_dir() / name

    @classmethod
    def models_dir(cls) -> Path:
        return cls.root() / "models"

    @classmethod
    def temp_dir(cls, date=None) -> Path:
        p = cls.root() / "tmp"
        if date:
            return p / date
        return p

    @classmethod
    def symbol_dir(cls, symbol, date=None) -> Path:
        p = cls.data_dir() / "symbol" / symbol
        if date is not None:
            return p / date

        return p

    @classmethod
    def order_dir(cls, symbol, date) -> Path:
        return cls.symbol_dir(symbol, date) / "Order.parquet"

    @classmethod
    def trade_dir(cls, symbol, date) -> Path:
        return cls.symbol_dir(symbol, date) / "Trade.parquet"

    @classmethod
    def snapshot_dir(cls, symbol, date) -> Path:
        return cls.symbol_dir(symbol, date) / "Snapshot.parquet"
