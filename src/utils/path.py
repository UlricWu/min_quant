#!filepath: src/utils/path.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

from src import logs


class PathManager:
    """
    PathManager（冷热分离 · FINAL / FROZEN）

    =========================
    物理存储布局（冻结）
    =========================

    /home/user
     ├── dev/
     │     └── code/src/...
     │
     ├── data/                     ← 热数据（SSD / 工作集）
     │     ├── feature/            ← 分钟级特征
     │     ├── label/              ← 分钟级标签
     │     ├── bar/
     │     │     └── 1m/            ← 分钟 bar
     │     ├── meta/               ← slice / manifest / index
     │     ├── training/           ← 训练中间产物
     │     ├── backtest/           ← 回测中间产物
     │     └── event/
     │
     ├── shared/
     │     ├── configs/
     │     ├── models/
     │     ├── pretrained/
     │     └── cache/
     │
     └── /mnt/cold/                ← 冷数据（HDD / 不可变事实）
           ├── raw/                ← *.csv.7z 原始数据
           └── l2_normalized/      ← normalize 后 L2 parquet

    =========================
    设计铁律（不可回退）
    =========================

    - raw（*.csv.7z）只存在于 HDD
    - normalize 后的 L2 parquet 只存在于 HDD
    - 分钟级 feature / bar / label / meta 只存在于 SSD
    - Step / Pipeline 永远不感知冷热
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
        """热数据根目录（SSD）"""
        return cls.base_dir() / "data"

    @classmethod
    def hdd_root(cls) -> Path:
        """冷数据根目录（HDD）"""
        return Path("/mnt/cold")

    # =========================================================
    # cold data (HDD / immutable)
    # =========================================================
    @classmethod
    def raw_dir(cls, date: str | None = None) -> Path:
        """
        原始数据（*.csv.7z）
        """
        p = cls.hdd_root() / "raw"
        return p / date if date else p

    @classmethod
    def l2_normalized_dir(cls, date: str | None = None) -> Path:
        """
        normalize 后的 L2 parquet（逐笔 / 盘口）

        语义：
          - 冷数据
          - 顺序访问
          - 用于 tick / execution replay
        """
        p = cls.hdd_root() / "l2_normalized"
        return p / date if date else p

    # =========================================================
    # hot data (SSD / working set)
    # =========================================================
    @classmethod
    def feature_dir(cls, date: str | None = None) -> Path:
        """分钟级特征"""
        p = cls.ssd_root() / "feature"
        return p / date if date else p

    @classmethod
    def label_dir(cls, date: str | None = None) -> Path:
        """分钟级标签"""
        p = cls.ssd_root() / "label"
        return p / date if date else p

    # @classmethod
    # def bar_1m_dir(cls, date: str | None = None) -> Path:
    #     """1 分钟 bar"""
    #     p = cls.ssd_root() / "bar" / "1m"
    #     return p / date if date else p

    @classmethod
    def meta_dir(cls, date: str | None = None) -> Path:
        """slice / manifest / index"""
        p = cls.ssd_root() / "meta"
        return p / date if date else p

    @classmethod
    def model_dir(cls, date: str | None = None) -> Path:
        """训练结果工作集"""
        p = cls.ssd_root() / "training"
        return p / date if date else p

    @classmethod
    def backtest_dir(cls, date: str | None = None) -> Path:
        """回测工作集"""
        p = cls.ssd_root() / "backtest"
        return p / date if date else p

    @classmethod
    def fact_dir(cls, date: str | None = None) -> Path:
        """事件缓存"""
        p = cls.ssd_root() / "fact"
        return p / date if date else p

    # =========================================================
    # shared
    # =========================================================
    @classmethod
    def shared_dir(cls) -> Path:
        return cls.base_dir() / "shared"

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
    # config resolution
    # =========================================================
    @classmethod
    def project_config_dir(cls) -> Path:
        return cls.root() / "code" / "src" / "config"

    @classmethod
    def shared_config_dir(cls) -> Path:
        return cls.shared_dir() / "configs"

    @classmethod
    def config_file(cls, name: str) -> Path:
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
