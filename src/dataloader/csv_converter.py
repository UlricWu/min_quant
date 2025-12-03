#!filepath: src/dataloader/csv_converter.py
# import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from pathlib import Path
from typing import Optional, Dict

from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src.utils.path import PathManager
from src.config.app_config import AppConfig
from src import logs


class CSVToParquetConverter:
    """
    CSV → Parquet 转换模块（强 schema、无 dtype 警告）
    """

    def __init__(self, parquet_root: Optional[Path] = None):
        self.cfg = AppConfig.load()
        self.parquet_root = parquet_root or PathManager.parquet_dir()
        FileSystem.ensure_dir(self.parquet_root)

    def _build_dtype_map(self, schema: Dict[str, str]):
        """
        根据 config 中的 schema 生成 pandas dtype map
        """
        pandas_map = {
            "string": "string",
            "float": "float64",
            "int": "Int64",  # 可空整数
        }

        dtype_map = {}
        for col, typ in schema.items():
            if typ not in pandas_map:
                raise ValueError(f"未知数据类型 {typ}（列: {col}）")
            dtype_map[col] = pandas_map[typ]

        return dtype_map

    @logs.catch()
    def convert(self, csv_path: Path, relative_dir: Optional[str] = None) -> Path:
        """
        转换 CSV → Parquet（强 schema，无 dtype warning）
        - 若 parquet 已存在 → 自动跳过（断点续跑）
        """

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 文件不存在: {csv_path}")

        # 输出目录
        out_dir = self.parquet_root / relative_dir if relative_dir else self.parquet_root
        FileSystem.ensure_dir(out_dir)

        parquet_path = out_dir / (csv_path.stem + ".parquet")

        # ============ ⭐ 新增 skip 逻辑 ⭐ ============
        if parquet_path.exists():
            logs.info(f"[Parquet] 已存在，跳过: {parquet_path}")
            return parquet_path

        logs.info(f"[Parquet] CSV → Parquet: {csv_path} → {parquet_path}")

        # # 读取 CSV（不让 pandas 推断 dtype）
        # df = pd.read_csv(
        #     csv_path,
        #     dtype=self.dtype_map,
        #     low_memory=False,        # 重要：禁用 chunk-based 推断
        #     na_filter=False,         # 重要：避免把空格当作 NA
        #     engine="c",              # 更快、更稳定
        # )
        # Arrow CSV 读取配置
        read_options = pacsv.ReadOptions(
            use_threads=True,
            block_size=1 << 20,  # 1MB streaming blocks
            skip_rows=0,
        )

        parse_options = pacsv.ParseOptions(
            delimiter=',',
            quote_char='"',
            escape_char=False,
            newlines_in_values=True,  # ← 允许跨行字段
        )

        # ✔ 在正常情况下 TickTime 应该是： 2025-11-07 09:40:41.750
        # ❌ 而数据源给你的是： 2025-11-07
        # 09:40:41.750
        convert_options = pacsv.ConvertOptions(
            null_values=["", "NA"],  # 统一 null
            strings_can_be_null=True,
            auto_dict_encode=True,
        )

        # 读取 CSV → Arrow Table（超快）
        table = pacsv.read_csv(
            csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

        # 写入 parquet（最快格式）
        pq.write_table(
            table,
            parquet_path,
            compression="zstd",
            use_dictionary=True,
        )

        # 写入 parquet
        logs.info(f"[Parquet] 完成写入 → {parquet_path}")

        return parquet_path
