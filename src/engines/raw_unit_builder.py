#!filepath: src/engine/raw_unit_builder.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


# =============================================================================
# Raw Unit (冻结数据结构)
# =============================================================================


# =============================================================================


# Raw Unit Builder（冻结模块）
# =============================================================================
class RawUnitBuilder:
    """
    RawUnitBuilder（FINAL / FROZEN）

    职责：
      - 识别 raw 文件的事实类型
      - 构造 RawUnit 列表

    冻结原则：
      - 仅基于文件名判断
      - 不访问文件内容
      - 不依赖 pipeline / meta
      - 规则集中、可审计
    """

    # --------------------------------------------------
    def build(self, zfile: Path) -> dict:
        """
        根据 raw 文件名，构造 RawUnit 列表
        """
        if zfile.suffix != ".7z":
            raise ValueError(f"[RawUnitBuilder] invalid raw file: {zfile}")

        file_type = self._detect_type(zfile.stem)

        if file_type == "SH_MIXED":
            return {"sh_order": zfile, "sh_trade": zfile}

        if file_type == "SH_ORDER":
            return {"sh_order": zfile}

        if file_type == "SH_TRADE":
            return {"sh_trade": zfile}

        if file_type == "SZ_ORDER":
            return {"sz_order": zfile}

        if file_type == "SZ_TRADE":
            return {"sz_trade": zfile}

        raise RuntimeError(
            f"[RawUnitBuilder] unsupported raw file type: {zfile.name}"
        )

    # --------------------------------------------------
    @staticmethod
    def _detect_type(stem: str) -> str:
        """
        基于文件名识别 raw 文件类型（冻结规则）
        """
        lower = stem.lower()

        if lower.startswith("sh_stock_ordertrade"):
            return "SH_MIXED"

        if lower.startswith("sh_order"):
            return "SH_ORDER"
        if lower.startswith("sh_trade"):
            return "SH_TRADE"

        if lower.startswith("sz_order"):
            return "SZ_ORDER"
        if lower.startswith("sz_trade"):
            return "SZ_TRADE"

        raise RuntimeError(f"[RawUnitBuilder] cannot detect type: {stem}")
