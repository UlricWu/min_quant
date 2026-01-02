#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import sys

DATA_ROOT = Path.home() / "data_handler"

TOP_DIRS = (
    "raw",
    "parquet",
    "fact",
    "meta",
    "feature",
    "label",
)


def is_effectively_empty(dir_path: Path) -> bool:
    """
    判断目录是否“有效为空”：

    - 不存在 → 视为 empty
    - 仅包含空子目录 → empty
    - 含任意文件 → 非 empty
    """
    if not dir_path.exists():
        return True

    for p in dir_path.rglob("*"):
        if p.is_file():
            return False

    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Remove empty date directories under ~/data_handler/*/<date>/.\n"
            "Scans all dates without requiring --date.\n"
            "Safe to run periodically or after PipelineAbort."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将要删除的目录，不真正删除",
    )

    args = parser.parse_args()

    if not DATA_ROOT.exists():
        print(f"[ERROR] 数据根目录不存在: {DATA_ROOT}")
        sys.exit(1)

    to_delete: set[Path] = set()

    # --------------------------------------------------
    # 扫描所有 <top>/<date> 目录
    # --------------------------------------------------
    for top in TOP_DIRS:
        top_dir = DATA_ROOT / top
        if not top_dir.exists():
            continue

        for date_dir in top_dir.iterdir():
            if not date_dir.is_dir():
                continue

            if is_effectively_empty(date_dir):
                to_delete.add(date_dir)

    if not to_delete:
        print("[OK] 未发现任何空 date 目录")
        return

    # --------------------------------------------------
    # 执行删除
    # --------------------------------------------------
    for path in sorted(to_delete):
        if args.dry_run:
            print(f"[DRY-RUN] would remove {path}")
            continue

        shutil.rmtree(path)
        print(f"[OK] removed {path}")

    if args.dry_run:
        print("[DRY-RUN] 完成（未实际删除）")
    else:
        print(f"[DONE] 共删除 {len(to_delete)} 个空 date 目录")


if __name__ == "__main__":
    main()

# scripts/cleanup_all_empty_dates.py 先看会删什么（强烈推荐）
# 真正执行清理 python scripts/cleanup_all_empty_dates.py
