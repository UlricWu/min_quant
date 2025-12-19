#!filepath: src/adapters/symbol_router_adapter.py
from __future__ import annotations

from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq

from src.adapters.base_adapter import BaseAdapter
from src.engines.symbol_router_engine import SymbolRouterEngine
from src import logs


FileType = Literal["order", "trade"]


class SymbolRouterAdapter(BaseAdapter):
    """
    SymbolRouterAdapter（契约最终版）

    允许：
        - 读 parquet
        - 调 Engine.split
        - 写 parquet 到 symbol 分区目录

    禁止：
        - symbol 白名单过滤（不关心股票池）
        - schema 映射（不处理 SecurityID -> Symbol）
        - 业务规则 / 时间语义
    """

    def __init__(
        self,
        *,
        engine: SymbolRouterEngine,
        inst=None,
        compression: str = "zstd",
    ) -> None:
        super().__init__(inst)
        self.engine = engine
        self.compression = compression

    def run(
        self,
        *,
        parquet_path: Path,
        out_root: Path,
        file_type: FileType,
        date: str,
    ) -> None:
        if not parquet_path.exists():
            raise FileNotFoundError(parquet_path)

        logs.info(f"[SymbolSplit] read: {parquet_path}")

        table = pq.read_table(parquet_path)
        split_map = self.engine.split(table)

        for symbol, sub_table in split_map.items():
            out_path = (
                out_root
                / symbol
                / file_type
                / f"date={date}.parquet"
            )
            out_path.parent.mkdir(parents=True, exist_ok=True)

            pq.write_table(
                sub_table,
                out_path,
                compression=self.compression,
            )

        logs.info(f"[SymbolSplit] done: {parquet_path.name} -> {len(split_map)} symbols")

# from __future__ import annotations
#
# from pathlib import Path
# from typing import Iterable
#
# from src import logs
# from src.adapters.base_adapter import BaseAdapter
# from src.engines.symbol_router_engine import SymbolRouterEngine
#
#
# class SymbolRouterAdapter(BaseAdapter):
#     """
#     Adapter = 策略 + 编排
#
#     - 决定处理哪些文件
#     - 决定哪些 symbol
#     - 调用 Engine 执行
#     - 在此层记录 leaf timer
#     """
#
#     def __init__(
#             self,
#             *,
#             engine: SymbolRouterEngine,
#             symbols: Iterable[str],
#             inst=None,
#     ):
#         super().__init__(inst)
#         self.engine = engine
#         self.symbols = {f"{int(s):06d}" for s in symbols}
#
#     def split(
#             self,
#             *,
#             date: str,
#             parquet_files: list[Path],
#             symbol_dir: Path,
#     ) -> None:
#         for p in parquet_files:
#             kind = self._infer_kind(p.name)
#             if kind is None:
#                 logs.warning(f"Parquet file {p} has no kind")
#                 continue
#
#             name_str = str(p).split("/")[-1]
#             # leaf timer（accounting 单元）
#             with self.timer(name_str):
#                 self.engine.route_file(
#                     date=date,
#                     kind=kind,
#                     parquet_path=p,
#                     symbol_dir=symbol_dir,
#                     symbols=self.symbols,
#                 )
#
#     @staticmethod
#     def _infer_kind(name: str) -> str | None:
#         n = name.lower()
#         if "order" in n:
#             return "Order"
#         if "trade" in n:
#             return "Trade"
#         return None
