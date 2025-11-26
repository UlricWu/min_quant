#!filepath: src/utils/filesystem.py
import os
import shutil
from pathlib import Path
from typing import List, Optional

from src import logs


class FileSystem:
    """
    统一文件系统工具
    - 自动创建目录
    - 安全写入文件（临时文件 → rename）
    - 删除文件/目录
    - 扫描目录
    - 获取文件大小等
    """

    @staticmethod
    def ensure_dir(path: str | Path) -> Path:
        """
        创建目录（如果不存在）
        """
        p = Path(path)
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            logs.debug(f"[FS] 创建目录: {p}")
        return p

    @staticmethod
    def file_exists(path: str | Path) -> bool:
        return Path(path).exists()

    @staticmethod
    def get_file_size(path: str | Path) -> int:
        """
        返回文件大小（字节）
        """
        p = Path(path)
        if not p.exists():
            return 0
        return p.stat().st_size

    @staticmethod
    def format_size(size_bytes: int) -> str:
        """
        将字节转换为可读格式（GB / MB）
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"

    @staticmethod
    def safe_write(path: str | Path, data: bytes) -> None:
        """
        原子写入（避免部分写入导致文件损坏）
        写入步骤：
            1) 先写入 tmp 文件
            2) rename → 正式文件
        """
        path = Path(path)
        FileSystem.ensure_dir(path.parent)

        tmp_path = path.with_suffix(".tmp")

        with open(tmp_path, "wb") as f:
            f.write(data)
            logs.debug(f"[FS] 写入临时文件: {tmp_path}")

        tmp_path.rename(path)
        logs.debug(f"[FS] 原子写入完成: {path}")

    @staticmethod
    def remove(path: str | Path) -> None:
        """
        安全删除文件/目录
        """
        p = Path(path)

        if not p.exists():
            logs.debug(f"[FS] 路径不存在，无需删除: {p}")
            return

        if p.is_dir():
            shutil.rmtree(p)
            logs.debug(f"[FS] 删除目录: {p}")
        else:
            p.unlink()
            logs.debug(f"[FS] 删除文件: {p}")

    @staticmethod
    def scan_dir(path: str | Path, suffix: Optional[str] = None) -> List[Path]:
        """
        返回目录下所有文件（可按后缀过滤）
        """
        p = Path(path)
        if not p.exists():
            return []

        files = []
        for f in p.iterdir():
            if f.is_file():
                if suffix is None or f.suffix == suffix:
                    files.append(f)

        return sorted(files)

    @staticmethod
    def get_dir_size(path: str | Path) -> int:
        """
        获取目录总大小（字节）
        """
        total = 0
        p = Path(path)

        if not p.exists():
            return 0

        for f in p.rglob("*"):
            if f.is_file():
                total += f.stat().st_size

        return total

    @staticmethod
    def clean_temp_files(path: str | Path, suffix=".tmp") -> int:
        """
        删除目录下所有 *.tmp 临时文件
        返回删除的数量
        """
        p = Path(path)
        count = 0

        if not p.exists():
            return 0

        for f in p.rglob(f"*{suffix}"):
            f.unlink()
            count += 1
            logs.debug(f"[FS] 删除临时文件: {f}")

        return count
