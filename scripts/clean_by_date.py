#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys


DATA_ROOT = Path.home() / "data" / "symbol"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean specified files under ~/data/symbol/*/<date>/"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="交易日，如 2025-11-04",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        required=True,
        help="要删除的文件名，如 Trade.parquet Order.parquet Snapshot.parquet",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将要删除的文件，不真正删除",
    )

    args = parser.parse_args()

    if not DATA_ROOT.exists():
        print(f"[ERROR] 数据根目录不存在: {DATA_ROOT}")
        sys.exit(1)

    symbol_dirs = sorted(p for p in DATA_ROOT.iterdir() if p.is_dir())
    if not symbol_dirs:
        print(f"[WARN] 未找到任何 symbol 目录: {DATA_ROOT}")
        sys.exit(0)

    total_deleted = 0

    for symbol_dir in symbol_dirs:
        date_dir = symbol_dir / args.date
        if not date_dir.exists():
            continue

        for fname in args.files:
            fpath = date_dir / fname
            if not fpath.exists():
                continue

            if args.dry_run:
                print(f"[DRY-RUN] {fpath}")
            else:
                fpath.unlink()
                total_deleted += 1
                print(f"[OK] deleted {fpath}")

    if args.dry_run:
        print("[DRY-RUN] 完成（未实际删除）")
    else:
        print(f"[DONE] 共删除 {total_deleted} 个文件")


if __name__ == "__main__":
    main()
# python scripts/clean_by_date.py  --date 2025-11-06 --files Trade_Enriched.parquet Order.parquet Snapshot.parquet
