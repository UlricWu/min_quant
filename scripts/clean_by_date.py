#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import shutil


DATA_ROOT = Path.home() / "data"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean data by date under ~/data/{canonical,parquet,symbol}/"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="交易日，如 2015-01-01",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        help=(
            "要删除的文件名（如 Trade.parquet Order.parquet）。\n"
            "若不指定，则删除 raw 以外的所有文件/目录。"
        ),
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

    targets: list[Path] = []

    # ------------------------------------------------------------------
    # canonical / parquet
    # ------------------------------------------------------------------
    for top in ("canonical", "parquet", 'meta'):
        date_dir = DATA_ROOT / top / args.date
        if not date_dir.exists():
            continue

        if args.files:
            # 只删指定文件，保留 <date> 目录
            targets.extend(date_dir / f for f in args.files)
        else:
            # 整天删除（包括 <date> 目录）
            targets.append(date_dir)

    # ------------------------------------------------------------------
    # symbol/*/<date>
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # symbol/<date>/<symbol>
    # ------------------------------------------------------------------
    symbol_date_dir = DATA_ROOT / "symbol" / args.date
    if symbol_date_dir.exists():
        if args.files:
            # 只删指定文件（保留 <date> 目录）
            for sym_dir in symbol_date_dir.iterdir():
                if not sym_dir.is_dir():
                    continue
                targets.extend(sym_dir / f for f in args.files)
        else:
            # 未指定 files：整天删除（包括 <date>）
            targets.append(symbol_date_dir)

    # ------------------------------------------------------------------
    # 执行删除
    # ------------------------------------------------------------------
    deleted = 0

    for path in sorted(set(targets)):
        if not path.exists():
            continue

        if args.dry_run:
            print(f"[DRY-RUN] {path}")
            continue

        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

        deleted += 1
        print(f"[OK] deleted {path}")

    if args.dry_run:
        print("[DRY-RUN] 完成（未实际删除）")
    else:
        print(f"[DONE] 共删除 {deleted} 个条目")


if __name__ == "__main__":
    main()


# python scripts/clean_by_date.py  --date 2015-01-01 --files Trade_Enriched.parquet Order.parquet Snapshot.parquet
