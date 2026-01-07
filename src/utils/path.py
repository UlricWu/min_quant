# src/utils/path.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

from src import logs


class PathManager:
    """
    PathManager (FINAL / FROZEN)

    ============================================================
    STORAGE SEMANTICS (CONSTITUTION)
    ============================================================

    1. train
       - 每一次 train = 一个 run
       - 只写 run-scoped 目录
       - 永远不影响 latest

    2. experiment
       - 唯一允许 promote
       - 将 train/{run_id} → shared/models/{model_name}/{version}
       - 更新 shared/models/{model_name}/latest

    3. backtest
       - 只读 shared/models/{model_name}/latest
       - 永远不读 train 目录

    ============================================================
    PHYSICAL LAYOUT (FROZEN)
    ============================================================

    /home/user
     ├── dev/
     │    └── code/src/...
     │
     ├── data/                    ← SSD / working set
     │    ├── feature/
     │    ├── label/
     │    ├── meta/
     │    ├── training/            ← train run outputs (EXPERIMENTS)
     │    │    └── {run_id}/
     │    ├── backtest/
     │    └── fact/
     │
     ├── shared/                  ← STABLE / CONSUMABLE
     │    ├── models/             ← ONLY published models
     │    │    └── {model_name}/
     │    │         ├── 2026-01-06/
     │    │         └── latest -> 2026-01-06
     │    ├── configs/
     │    ├── pretrained/
     │    └── cache/
     │
     └── /mnt/cold/               ← HDD / immutable facts
          ├── raw/
          └── l2_normalized/
    """

    _root: Optional[Path] = None

    # ============================================================
    # root detection
    # ============================================================
    @classmethod
    def detect_root(cls) -> Path:
        """
        Current file:
            /home/user/dev/code/src/utils/path.py

        root = parents[3] = /home/user/dev
        """
        current = Path(__file__).resolve()
        try:
            root = current.parents[3]
            logs.debug(f"[PathManager] detect_root = {root}")
            return root
        except Exception:
            logs.warning("[PathManager] detect_root failed, fallback cwd()")
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

    # ============================================================
    # base
    # ============================================================
    @classmethod
    def base_dir(cls) -> Path:
        """
        root = /home/user/dev
        base = /home/user
        """
        return cls.root().parent

    # ============================================================
    # physical roots
    # ============================================================
    @classmethod
    def ssd_root(cls) -> Path:
        """SSD / working set"""
        return cls.base_dir() / "data"

    @classmethod
    def hdd_root(cls) -> Path:
        """HDD / immutable facts"""
        return Path("/mnt/cold")

    # ============================================================
    # cold data (HDD)
    # ============================================================
    @classmethod
    def raw_dir(cls, date: str | None = None) -> Path:
        p = cls.hdd_root() / "raw"
        return p / date if date else p

    @classmethod
    def l2_normalized_dir(cls, date: str | None = None) -> Path:
        p = cls.hdd_root() / "l2_normalized"
        return p / date if date else p

    # ============================================================
    # hot data (SSD)
    # ============================================================
    @classmethod
    def feature_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "feature"
        return p / date if date else p

    @classmethod
    def label_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "label"
        return p / date if date else p

    @classmethod
    def meta_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "meta"
        return p / date if date else p

    # ============================================================
    # training (EXPERIMENTS ONLY)
    # ============================================================
    @classmethod
    def train_run_dir(cls, run_id: str) -> Path:
        """
        Train experiment output (run-scoped).

        - Produced by `train`
        - NOT published
        - NOT consumed by backtest
        """
        return cls.ssd_root() / "training" / run_id

    # Backward compatibility (deprecated)
    @classmethod
    def train_dir(cls, run_id: str, date: str | None = None) -> Path:
        logs.warning(
            "[PathManager] train_dir() is deprecated, "
            "use train_run_dir(run_id)"
        )
        p = cls.train_run_dir(run_id)
        return p / date if date else p

    # ============================================================
    # backtest working set
    # ============================================================
    @classmethod
    def backtest_dir(cls, run_id: str | None = None) -> Path:
        p = cls.ssd_root() / "backtest"
        return p / run_id if run_id else p

    @classmethod
    def fact_dir(cls, date: str | None = None) -> Path:
        p = cls.ssd_root() / "fact"
        return p / date if date else p

    # ============================================================
    # shared (STABLE / CONSUMABLE)
    # ============================================================
    @classmethod
    def shared_dir(cls) -> Path:
        return cls.base_dir() / "shared"

    @classmethod
    def models_dir(cls) -> Path:
        return cls.shared_dir() / "models"

    @classmethod
    def model_lineage_dir(cls, model_name: str) -> Path:
        """
        Published model lineage root.

        Example:
            shared/models/minute_sgd_online_v1/
        """
        return cls.models_dir() / model_name

    @classmethod
    def model_version_dir(cls, model_name: str, version: str) -> Path:
        """
        One published model version.

        Example:
            shared/models/minute_sgd_online_v1/2026-01-06/
        """
        return cls.model_lineage_dir(model_name) / version

    @classmethod
    def model_latest_dir(cls, model_name: str) -> Path:
        """
        Latest published model (symlink).

        MUST exist for backtest / inference.
        """
        return cls.model_lineage_dir(model_name) / "latest"

    @classmethod
    def pretrained_dir(cls) -> Path:
        return cls.shared_dir() / "pretrained"

    @classmethod
    def cache_dir(cls) -> Path:
        return cls.shared_dir() / "cache"

    # ============================================================
    # config resolution
    # ============================================================
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

    # ============================================================
    # misc helpers
    # ============================================================
    @classmethod
    def str_symbol(cls, symbol: str | int) -> str:
        """统一 symbol 目录名（000001 形式）"""
        return f"{int(symbol):06d}"
