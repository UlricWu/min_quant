#!filepath: src/engines/symbol_router_engine.py

from __future__ import annotations
import pyarrow as pa
import pyarrow.compute as pc
from typing import Dict, List, Iterable, Optional


class SymbolRouterEngine:
    """
    纯逻辑 Engine：
    - 负责 symbol 过滤（0/3/6 前缀）
    - 负责将 Arrow batch 拆成 {symbol: sub_batch}
    - 不负责：
        * parquet I/O
        * 目录创建
        * PathManager
    """

    ALLOWED_PREFIX = ("0", "3", "6")

    def __init__(self, symbols: Optional[Iterable[int | str]]):
        # cfg.data.symbols 可能是 int 或 str
        if symbols:
            self.symbols = [int(s) for s in symbols]
            self.symbol_str_set = {f"{int(s):06d}" for s in symbols}
            self.symbol_str_to_cfg_symbol = {
                f"{int(s):06d}": int(s) for s in symbols
            }
        else:
            self.symbols = None
            self.symbol_str_set = set()
            self.symbol_str_to_cfg_symbol = {}

    # ----------------------------------------------------------------------
    def split_batch_by_symbol(
        self,
        batch: pa.RecordBatch,
        symbol_col: str = "SecurityID",
    ) -> Dict[int, pa.RecordBatch]:

        result: Dict[int, pa.RecordBatch] = {}

        # 找到 symbol 列
        idx = batch.schema.get_field_index(symbol_col)
        sym_arr = batch.column(idx)

        # 确保转换成 string
        if not pa.types.is_string(sym_arr.type):
            sym_arr = sym_arr.cast(pa.string())
            batch = batch.set_column(idx, symbol_col, sym_arr)

        # batch 中实际出现的 symbol
        unique_syms = pc.unique(sym_arr).to_pylist()

        for sid_str in unique_syms:
            if not sid_str:
                continue

            # 仅处理 0/3/6 前缀的股票
            if sid_str[0] not in self.ALLOWED_PREFIX:
                continue

            # 如果配置了指定 symbols，则只保留其内
            if self.symbol_str_set and sid_str not in self.symbol_str_set:
                continue

            cfg_symbol = self.symbol_str_to_cfg_symbol.get(sid_str)
            if cfg_symbol is None:
                continue

            # 过滤出该 symbol 的数据
            mask = pc.equal(sym_arr, pa.scalar(sid_str))
            sub_batch = batch.filter(mask)

            if sub_batch.num_rows > 0:
                result[cfg_symbol] = sub_batch

        return result
