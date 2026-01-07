#!/usr/bin/env python3
from __future__ import annotations

import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Set

import pandas as pd

from src.utils.path import PathManager


# =============================================================================
# 配置
# =============================================================================
pm = PathManager()

SRC_START_DATE = "2025-11-03"
SRC_END_DATE   = "2025-12-03"

DST_START_DATE = "2005-11-03"

SAMPLE_SYMBOLS = 100
SYMBOL_COL = "symbol"
RANDOM_SEED = 42


# =============================================================================
# 文件名映射（冻结规则）
# =============================================================================
def feature_to_label_name(feature_name: str) -> str:
    """
    feature.xxx.parquet -> label.xxx.parquet
    """
    if not feature_name.startswith("feature."):
        raise ValueError(f"invalid feature filename: {feature_name}")
    return "label." + feature_name[len("feature."):]
# =============================================================================
# 工具函数
# =============================================================================
def date_range(start: str, end: str):
    cur = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur <= end_dt:
        yield cur
        cur += timedelta(days=1)


def shift_date(src_dt: datetime, src_start: str, dst_start: str) -> str:
    src0 = datetime.strptime(src_start, "%Y-%m-%d")
    dst0 = datetime.strptime(dst_start, "%Y-%m-%d")
    return (dst0 + (src_dt - src0)).strftime("%Y-%m-%d")
# =============================================================================
# 核心逻辑
# =============================================================================
def sample_symbol_pool(src_date: str) -> Dict[str, Set[str]]:
    """
    在起始日抽取 symbol pool
    key = feature filename
    value = sampled symbols (feature ∩ label)
    """
    random.seed(RANDOM_SEED)

    feat_dir = pm.feature_dir(src_date)
    lab_dir = pm.label_dir(src_date)

    pools: Dict[str, Set[str]] = {}

    for feat_file in sorted(feat_dir.glob("feature.*.parquet")):
        feat_name = feat_file.name
        lab_name = feature_to_label_name(feat_name)
        lab_file = lab_dir / lab_name

        if not lab_file.exists():
            raise FileNotFoundError(f"label file missing: {lab_file}")

        print(f"[POOL] sampling symbols from {feat_name}")

        feat_syms = set(
            pd.read_parquet(
                feat_file, columns=[SYMBOL_COL]
            )[SYMBOL_COL].unique()
        )
        lab_syms = set(
            pd.read_parquet(
                lab_file, columns=[SYMBOL_COL]
            )[SYMBOL_COL].unique()
        )

        symbols = sorted(feat_syms & lab_syms)

        if len(symbols) < SAMPLE_SYMBOLS:
            raise ValueError(
                f"{feat_name}: not enough symbols "
                f"({len(symbols)} < {SAMPLE_SYMBOLS})"
            )

        pools[feat_name] = set(
            random.sample(symbols, SAMPLE_SYMBOLS)
        )

        print(f"  → {feat_name}: {len(pools[feat_name])} symbols")

    return pools
def filter_one_day(
    *,
    src_date: str,
    dst_date: str,
    symbol_pools: Dict[str, Set[str]],
) -> None:
    feat_src = pm.feature_dir(src_date)
    lab_src = pm.label_dir(src_date)

    feat_dst = pm.feature_dir(dst_date)
    lab_dst = pm.label_dir(dst_date)

    feat_dst.mkdir(parents=True, exist_ok=True)
    lab_dst.mkdir(parents=True, exist_ok=True)

    print(f"[DAY] {src_date} → {dst_date}")

    for feat_name, symbols in symbol_pools.items():
        lab_name = feature_to_label_name(feat_name)

        feat_file = feat_src / feat_name
        lab_file = lab_src / lab_name

        if not feat_file.exists() or not lab_file.exists():
            print(f"  [SKIP] missing {feat_name} / {lab_name}")
            continue

        feat_df = pd.read_parquet(feat_file)
        lab_df = pd.read_parquet(lab_file)

        feat_out = feat_df[feat_df[SYMBOL_COL].isin(symbols)]
        lab_out = lab_df[lab_df[SYMBOL_COL].isin(symbols)]

        feat_out.to_parquet(feat_dst / feat_name, index=False)
        lab_out.to_parquet(lab_dst / lab_name, index=False)

        print(
            f"  {feat_name}: "
            f"{len(symbols)} symbols | "
            f"feature={len(feat_out)} | "
            f"label={len(lab_out)}"
        )
def main() -> None:
    print(
        f"""
Sample feature + label by symbol (range)

Source:
  {SRC_START_DATE} → {SRC_END_DATE}

Target:
  {DST_START_DATE} → (auto)

Symbols per file:
  {SAMPLE_SYMBOLS}
"""
    )

    # 1. 抽 symbol pool（只一次）
    symbol_pools = sample_symbol_pool(SRC_START_DATE)

    # 2. 复用 pool 生成时间序列
    for src_dt in date_range(SRC_START_DATE, SRC_END_DATE):
        src_date = src_dt.strftime("%Y-%m-%d")
        dst_date = shift_date(src_dt, SRC_START_DATE, DST_START_DATE)

        filter_one_day(
            src_date=src_date,
            dst_date=dst_date,
            symbol_pools=symbol_pools,
        )

    print("\n✅ Feature + Label sampling DONE.")


if __name__ == "__main__":
    main()
