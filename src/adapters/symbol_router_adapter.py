# from __future__ import annotations
from pathlib import Path
# import pyarrow as pa
# import pyarrow.compute as pc
# import pyarrow.dataset as ds
# import pyarrow.parquet as pq
#
from src.adapters.base_adapter import BaseAdapter
from src.utils.filesystem import FileSystem
from src import logs
#
#
class SymbolRouterAdapter(BaseAdapter):
#
    ALLOWED_PREFIX = ("0", "3", "6")

    def __init__(self, symbols, inst=None):
        super().__init__(inst)
        self.symbols = {f"{int(s):06d}" for s in symbols}

#
    def split(self, date: str):
        """
        按 symbol 拆分每日大表（流式）
        """
        pass


#         for p in files:
#             kind = self._infer_kind(p.name)
#             if kind is None:
#                 continue
#             self._route_file(date, kind, p, symbol_dir)
#
#     @staticmethod
#     def _infer_kind(name: str):
#         n = name.lower()
#         if "order" in n: return "Order"
#         if "trade" in n: return "Trade"
#         return None
#
#     # --------------------------------------------------
#     # 核心逻辑（流式拆分）
#     # --------------------------------------------------
#     def _route_file(self, date: str, kind: str, parquet_path: Path, symbol_dir: Path):
#
#         logs.info(f"[SymbolRouter] processing {parquet_path.name}")
#
#         with self.timer("symbolrouter_open"):
#             dataset = ds.dataset(parquet_path)
#
#         writers = {}
#
#         for batch in dataset.to_batches():
#             sym_arr = batch["SecurityID"].cast(pa.string())
#             unique_syms = pc.unique(sym_arr).to_pylist()
#
#             for sid in unique_syms:
#                 if sid not in self.symbols:
#                     continue
#
#                 mask = pc.equal(sym_arr, sid)
#                 sub = batch.filter(mask)
#
#                 writer = writers.get(sid)
#                 if writer is None:
#                     out = symbol_dir / sid / date
#                     FileSystem.ensure_dir(out)
#                     out_parquet = out / f"{kind}.parquet"
#                     writer = pq.ParquetWriter(out_parquet, sub.schema)
#                     writers[sid] = writer
#
#                 with self.timer("symbolrouter_write"):
#                     writer.write_batch(sub)
#
#         for w in writers.values():
#             w.close()
