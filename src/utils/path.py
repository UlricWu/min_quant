#!filepath: src/utils/path.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

from src import logs


class PathManager:
    """
    PathManager（冷热分离 · 冻结版）

    物理存储布局（当前）：

    /home/user
     ├── dev/
     │     └── code/src/...
     ├── data_ssd/              ← 热数据（SSD / 计算路径）
     │     ├── parquet/
     │     ├── fact/
     │     ├── feature/
     │     ├── meta/
     │     ├── label/
     │     ├── event/
     │     └── bar/
     ├── shared/
     │     ├── configs/
     │     ├── models/
     │     ├── pretrained/
     │     └── cache/
     └── /mnt/cold/             ← 冷数据（HDD / 不可变事实）
           └── raw/

    设计铁律（不可回退）：
      - raw（*.csv.7z）只存在于 /mnt/cold
      - parquet / fact / feature / meta 只存在于 SSD
      - 所有 Step / Pipeline 不感知冷热
      - 存储策略只能在 PathManager 中修改
    """

    _root: Optional[Path] = None

    # =========================================================
    # root detection
    # =========================================================
    @classmethod
    def detect_root(cls) -> Path:
        """
        自动检测项目 root。

        当前文件通常位于：
            /home/user/dev/code/src/utils/path.py

        因此：
            root = parents[3] = /home/user/dev
        """
        current = Path(__file__).resolve()
        try:
            root = current.parents[3]
            logs.debug(f"[PathManager] detect_root = {root}")
            return root
        except Exception:
            logs.warning("[PathManager] detect_root 失败，回退到 cwd()")
            return Path.cwd()

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

    # =========================================================
    # base dirs
    # =========================================================
    @classmethod
    def base_dir(cls) -> Path:
        """
        root = /home/user/dev
        base = /home/user
        """
        return cls.root().parent

    # =========================================================
    # physical storage roots
    # =========================================================
    @classmethod
    def ssd_root(cls) -> Path:
        """
        热数据根目录（SSD）

        说明：
          - 当前位于 /home/user/data
          - 未来可无痛切换到 /mnt/hot
        """
        return cls.base_dir() / "data"

    @classmethod
    def hdd_root(cls) -> Path:
        """
        冷数据根目录（HDD）

        说明：
          - 明确使用真实挂载点
          - 不要求目录名与项目结构一致
        """
        return Path("/mnt/cold")

    # =========================================================
    # cold data (HDD)
    # =========================================================
    @classmethod
    def raw_dir(cls, date: str | None = None) -> Path:
        """
        原始数据（*.csv.7z）

        语义：
          - 不可变事实
          - 长期保存
          - 仅 Download / Convert 使用
        """
        p = cls.hdd_root() / "raw"
        return p / date if date else p

    # =========================================================
    # hot data (SSD)
    # =========================================================
    @classmethod
    def parquet_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "parquet"
        return p / date if date else p

    @classmethod
    def fact_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "fact"
        return p / date if date else p

    @classmethod
    def meta_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "meta"
        return p / date if date else p

    @classmethod
    def feature_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "feature"
        return p / date if date else p

    @classmethod
    def label_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "label"
        return p / date if date else p

    @classmethod
    def event_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "event"
        return p / date if date else p

    @classmethod
    def bar_1m_root(cls) -> Path:
        return cls.ssd_root() / "bar" / "1m"

    # =========================================================
    # shared/
    # =========================================================
    @classmethod
    def shared_dir(cls) -> Path:
        return cls.base_dir() / "shared"

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

    # =========================================================
    # config（优先级：项目内 > shared）
    # =========================================================
    @classmethod
    def project_config_dir(cls) -> Path:
        """
        项目内部配置：
          /home/user/dev/code/src/config
        """
        return cls.root() / "code" / "src" / "config"

    @classmethod
    def shared_config_dir(cls) -> Path:
        """
        共享配置：
          /home/user/shared/configs
        """
        return cls.shared_dir() / "configs"

    @classmethod
    def config_file(cls, name: str) -> Path:
        """
        配置查找优先级：
          1) 项目内部 src/config
          2) shared/configs
        """
        p1 = cls.project_config_dir() / name
        if p1.exists():
            return p1
        return cls.shared_config_dir() / name

    # =========================================================
    # misc helpers
    # =========================================================
    @classmethod
    def str_symbol(cls, symbol: str | int) -> str:
        """统一 symbol 目录名（000001 形式）"""
        return f"{int(symbol):06d}"
