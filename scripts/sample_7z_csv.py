#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import random
from typing import List
from src.utils.path import PathManager
from pathlib import Path

# ================== 配置 ==================
pm = PathManager()
INPUT_FILES = [
    "SH_Stock_OrderTrade.csv.7z",
    "SZ_Order.csv.7z",
    "SZ_Trade.csv.7z",
]
INPUT_DATE = '2025-12-01'
OUTPUT_DATE = "2015-01-01"


SAMPLE_ROWS = 50_000_00  # ← 每个文件抽多少行，自己调
RANDOM_SEED = 42


# ==========================================


def stream_csv_lines(zfile: Path):
    """
    使用 7z -so 流式读取 csv 行
    """
    proc = subprocess.Popen(
        ["7z", "x", "-so", str(zfile)],
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        yield line
    proc.wait()


def reservoir_sample(lines, k: int) -> List[str]:
    """
    标准 reservoir sampling
    """
    sample = []
    for i, line in enumerate(lines):
        if i < k:
            sample.append(line)
        else:
            j = random.randint(0, i)
            if j < k:
                sample[j] = line
    return sample


def process_file(zfile: Path, out_dir: Path):
    print(f"[SAMPLE] {zfile.name}")

    lines = stream_csv_lines(zfile)

    # header
    header = next(lines)

    # sample
    body = reservoir_sample(lines, SAMPLE_ROWS)

    out_csv = out_dir / zfile.name.replace(".7z", "")
    out_7z = out_dir / zfile.name
    print(out_csv)
    # 写 CSV
    with out_csv.open("w") as f:
        f.write(header)
        for line in body:
            f.write(line)

    # 打包 7z（覆盖）
    subprocess.run(
        ["7z", "a", "-t7z", "-mx=1", str(out_7z), str(out_csv)],
        check=True,
    )

    out_csv.unlink()
    print(f"  → {out_7z} ({len(body)} rows)")


def main():
    random.seed(RANDOM_SEED)
    input_dir = pm.raw_dir(INPUT_DATE)
    out_dir = pm.raw_dir(OUTPUT_DATE)
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in list(input_dir.glob("*.7z")):
        process_file(name, out_dir)

    print("\n✅ Sample data ready:")
    print(out_dir.resolve())


if __name__ == "__main__":
    main()
