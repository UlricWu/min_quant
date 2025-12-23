#!filepath: src/utils/path.py
from pathlib import Path
from typing import Optional

from src import logs


class PathManager:
    """
    服务器目录结构：

    /home/wsw
     ├── dev/
     │     └── code/src/...
     ├── data/
     └── shared/

    root = /home/wsw/dev
    data_dir = root.parent / data
    shared_dir = root.parent / shared
    """

    _root: Optional[Path] = None

    # ---------------------------------------------------------
    # root detection
    # ---------------------------------------------------------
    @classmethod
    def detect_root(cls) -> Path:
        """
        自动检测 root:
        当前文件通常位于：
            /home/wsw/dev/code/src/utils/path.py
        因此 root = parents[3] = /home/wsw/dev
        """
        current = Path(__file__).resolve()

        try:
            root = current.parents[3]
            logs.debug(f"[PathManager] detect_root = {root}")
            return root
        except Exception:
            logs.warning("[PathManager] detect_root 失败，使用 cwd()")
            return Path.cwd()

    @classmethod
    def str_symbol(cls, symbol: str):
        return f"{int(symbol):06d}"

    @classmethod
    def root(cls) -> Path:
        if cls._root is None:
            cls._root = cls.detect_root()
        return cls._root

    @classmethod
    def set_root(cls, new_root: Path | str | None):
        if new_root is None:
            cls._root = None
        else:
            cls._root = Path(new_root).resolve()
        logs.debug(f"[PathManager] set_root = {cls._root}")

    # ---------------------------------------------------------
    # Top-level dirs
    # ---------------------------------------------------------
    @classmethod
    def base_dir(cls) -> Path:
        """
        root = /home/wsw/dev
        base_dir = /home/wsw
        """
        return cls.root().parent

    @classmethod
    def data_dir(cls) -> Path:
        return cls.base_dir() / "data"

    @classmethod
    def shared_dir(cls) -> Path:
        return cls.base_dir() / "shared"

    # ---------------------------------------------------------
    # data/
    # ---------------------------------------------------------
    @classmethod
    def raw_dir(cls, date=None) -> Path:
        p = cls.data_dir() / "raw"
        return p / date if date else p

    # @classmethod
    # def tmp_dir(cls, date=None) -> Path:
    #     p = cls.data_dir() / "tmp"
    #     return p / date if date else p

    @classmethod
    def parquet_dir(cls, date=None) -> Path:
        p = cls.data_dir() / "parquet"
        return p / str(date) if date else p

    @classmethod
    def fact_dir(cls, date:str) -> Path:
        if date:
            return cls.data_dir() / "fact" / date
        return cls.data_dir() / "fact"



    # @classmethod
    # def symbol_dir(cls, symbol: str, date: str | None = None) -> Path:
    #     p = cls.data_dir() / "symbol" / cls.str_symbol(symbol)
    #     return p / date if date else p
    #
    # @classmethod
    # def order_dir(cls, symbol, date):
    #     return cls.symbol_dir(symbol, date) / "Order.parquet"
    #
    # @classmethod
    # def trade_dir(cls, symbol, date):
    #     return cls.symbol_dir(symbol, date) / "Trade.parquet"
    #
    # @classmethod
    # def snapshot_dir(cls, symbol, date):
    #     return cls.symbol_dir(symbol, date) / "Snapshot.parquet"

    # ---------------------------------------------------------
    # shared/
    # ---------------------------------------------------------
    @classmethod
    def shared_data_dir(cls) -> Path:
        return cls.shared_dir() / "data"

    @classmethod
    def models_dir(cls) -> Path:
        return cls.shared_dir() / "models"

    @classmethod
    def pretrained_dir(cls) -> Path:
        return cls.shared_dir() / "pretrained"

    @classmethod
    def cache_dir(cls) -> Path:
        return cls.shared_dir() / "cache"

    # ---------------------------------------------------------
    # config (priority: src/config > shared/configs)
    # ---------------------------------------------------------
    @classmethod
    def project_config_dir(cls) -> Path:
        """项目内部配置：dev/code/src/config/"""
        return cls.root() / "code" / "src" / "config"

    @classmethod
    def shared_config_dir(cls) -> Path:
        """共享配置：/home/wsw/shared/configs"""
        return cls.shared_dir() / "configs"

    @classmethod
    def config_file(cls, name: str) -> Path:
        """
        优先级：
            1) 项目内部 src/config
            2) shared/configs
        """
        p1 = cls.project_config_dir() / name
        if p1.exists():
            return p1

        return cls.shared_config_dir() / name

    # PathManager 中（示意）
    @classmethod
    def bar_1m_root(cls) -> Path:
        return cls.data_dir() / "bar" / "1m"

    @classmethod
    def canonical_dir(cls, date: str = '') -> Path:
        if date:
            return cls.data_dir() / "canonical" / date
        return cls.data_dir() / "canonical"

    @classmethod
    def meta_dir(cls, date: str = '') -> Path:
        if date:
            return cls.data_dir() / "meta" / date
        return cls.data_dir() / "meta"
