#!filepath: src/dataloader/decompressor.py
import shutil
from pathlib import Path
from typing import Union

import py7zr

from src.utils.filesystem import FileSystem
from src import logs


class Decompressor:
    """
    7z 解压器（生产级）：
    - 支持传入 Path（.7z 文件）
    - 支持传入文件夹（批量处理所有 .7z）
    - 自动扁平化目录结构
    - 返回所有生成的 CSV 文件路径列表
    """

    def extract_7z(self, src: Union[str, Path], subdir: str):
        """
        解压单个 .7z 文件（不做任何结构修改）

        Parameters
        ----------
        src: str | Path
            7z 文件路径
        subdir: str
            解压目标目录（例如 tmp/decompress/<date>）

        Returns
        -------
        Path: 解压所在目录
        """

        # 目标输出目录
        src = Path(src)
        out_dir = Path(subdir)
        FileSystem.ensure_dir(out_dir)

        # ==========================================================
        # 如果 src 是文件 → 解压单个文件
        # ==========================================================

        if src.is_file() and src.suffix == ".7z":
            self._extract_single_file(src, out_dir)
            return
        # todo, 输入是文件夹
    @logs.catch()
    def _extract_single_file(self, src_7z: Path, out_dir: Path) :
        """
        解压单个 7z，并扁平化输出为 out_dir/*.csv

        Returns
        -------
        List[Path] : 解压后所有 CSV 文件路径
        """
        logs.info(f"[Decompress] 解压 {src_7z} → {out_dir}")
        with py7zr.SevenZipFile(src_7z, mode='r') as archive:
            archive.extract(path=out_dir)

        logs.info(f"[Decompress] 解压成功 {out_dir}")
