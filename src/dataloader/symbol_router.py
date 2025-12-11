# #!filepath: src/dataloader/symbol_router.py
#
# from __future__ import annotations
#
# from collections import defaultdict
# from pathlib import Path
# from typing import Iterable, Optional, Dict
#
# import pyarrow as pa
# import pyarrow.compute as pc
# import pyarrow.dataset as ds
# import pyarrow.parquet as pq
#
# from src import logs
# from src.utils.filesystem import FileSystem
# from src.utils.path import PathManager
# from src.dataloader.router_metadata import RouterMetadata
#
#
# class SymbolRouter:
#     """
#     高性能 Streaming Symbol Router
#
#     功能：
#     - 对每日 parquet（SH_Order / SH_Trade / SZ_Order / SZ_Trade）只扫描一遍
#     - 按 symbol 流式拆分写入 data/symbol/<symbol>/<date>/Order.parquet|Trade.parquet
#     - 仅处理 cfg.data.symbols 中配置的股票（避免无意义拆分）
#     - 保持 ALLOWED_PREFIX 过滤规则（0/3/6）
#
#     设计要点：
#     - 使用 pyarrow.dataset.dataset + to_batches() 流式读取 parquet
#     - 使用 ParquetWriter.write_batch 增量写入
#     - 不再使用 read_table / pandas / 多次 filter 全表
#
#     注意：
#     - PathManager 由外部注入（依赖注入），方便测试及未来替换路径策略
#     """
#
#     # 只处理这几个前缀的股票
#     ALLOWED_PREFIX = ("0", "3", "6")
#
#     def __init__(
#             self,
#             symbols: Optional[Iterable[int | str]],
#             path_manager: PathManager,
#     ) -> None:
#         self.meta = RouterMetadata()
#         self.path_manager = path_manager
#
#         # cfg.data.symbols 可能是 int 或 str，这里统一构造成：
#         #   - self.symbols: List[int] 原样保留（方便和外部保持一致）
#         #   - self.symbol_str_set: Set[str] 六位字符串，如 "603322"
#         #   - self.symbol_str_to_cfg_symbol: "603322" -> 原始 symbol（int）
#         if symbols:
#             self.symbols = [int(s) for s in symbols]
#             self.symbol_str_set = {f"{int(s):06d}" for s in symbols}
#             self.symbol_str_to_cfg_symbol: Dict[str, int] = {
#                 f"{int(s):06d}": int(s) for s in symbols
#             }
#         else:
#             self.symbols = None
#             self.symbol_str_set = set()
#             self.symbol_str_to_cfg_symbol = {}
#
#     # ------------------------------------------------------------------
#     # 对某个日期执行路由拆分
#     # ------------------------------------------------------------------
#     def split(self,a,b,c):
#         pass
#     def route_date(self, date: str) -> None:
#         pass
#         # PathManager.parquet_dir() 返回根目录，这里手动拼接 date
#     #     date_dir = self.path_manager.parquet_dir() / date
#     #     if not date_dir.exists():
#     #         logs.warning(f"[SymbolRouter] date_dir={date_dir} 不存在，跳过")
#     #         return
#     #
#     #     logs.info(f"[SymbolRouter] ==== Symbol router date={date} ====")
#     #     self.meta.reset()
#     #
#     #     parquet_files = sorted(date_dir.glob("*.parquet"))
#     #     if not parquet_files:
#     #         logs.warning(f"[SymbolRouter] {date_dir} 下没有 parquet 文件")
#     #         return
#     #
#     #     for parquet_file in parquet_files:
#     #         name = parquet_file.name
#     #
#     #         # 识别 Order / Trade 类型（根据文件名约定）
#     #         kind = self._infer_kind_from_filename(name)
#     #         if kind is None:
#     #             logs.debug(f"[SymbolRouter] 跳过无关文件: {name}")
#     #             continue
#     #
#     #         self._route_single_parquet(parquet_file, date, kind)
#     #
#     #     logs.info(f"[SymbolRouter] ==== 完成 {date} ====")
#     #
#     # # ------------------------------------------------------------------
#     # # 根据文件名识别是 Order 还是 Trade
#     # # ------------------------------------------------------------------
#     # @staticmethod
#     # def _infer_kind_from_filename(filename: str) -> Optional[str]:
#     #     lower = filename.lower()
#     #     # 示例：SH_Order.parquet, SH_Trade.parquet, SZ_Order.parquet, SZ_Trade.parquet
#     #     if "order" in lower:
#     #         return "Order"
#     #     if "trade" in lower:
#     #         return "Trade"
#     #     return None
#     #
#     # # ------------------------------------------------------------------
#     # # 核心：对单个 parquet 文件做「一次扫描 → 多 symbol 流式拆分」
#     # # ------------------------------------------------------------------
#     # def _route_single_parquet(
#     #         self,
#     #         parquet_path: Path,
#     #         date: str,
#     #         kind: str,
#     #         symbol_col: str = "SecurityID",
#     # ) -> None:
#     #     logs.info(f"[SymbolRouter] 开始拆分: {parquet_path}")
#     #
#     #     try:
#     #         dataset = ds.dataset(parquet_path, format="parquet")
#     #     except Exception as e:
#     #         logs.exception(f"[SymbolRouter] 无法打开 parquet: {parquet_path}，错误: {e}")
#     #         return
#     #
#     #     writers: Dict[int, pq.ParquetWriter] = {}
#     #     row_counts: Dict[int, int] = defaultdict(int)
#     #
#     #     total_batches = 0
#     #     total_rows = 0
#     #
#     #     for batch in dataset.to_batches():
#     #         total_batches += 1
#     #         total_rows += batch.num_rows
#     #
#     #         if symbol_col not in batch.schema.names:
#     #             raise KeyError(
#     #                 f"[SymbolRouter] parquet 缺少列 {symbol_col}: {parquet_path}"
#     #             )
#     #
#     #         # 确保 symbol 列是 string 类型
#     #         idx = batch.schema.get_field_index(symbol_col)
#     #         sym_arr = batch.column(idx)
#     #         if not pa.types.is_string(sym_arr.type):
#     #             sym_arr = sym_arr.cast(pa.string())
#     #             batch = batch.set_column(idx, symbol_col, sym_arr)
#     #
#     #         # 当前 batch 中实际出现过的 symbol（避免对整个 cfg.symbols 逐个过滤）
#     #         unique_syms = pc.unique(sym_arr).to_pylist()
#     #
#     #         for sid_str in unique_syms:
#     #             if not sid_str:
#     #                 continue
#     #
#     #             # 过滤不在 0/3/6 前缀的
#     #             if sid_str[0] not in self.ALLOWED_PREFIX:
#     #                 continue
#     #
#     #             # 若传入了 symbol 列表，则只保留其中的
#     #             if self.symbol_str_set and sid_str not in self.symbol_str_set:
#     #                 continue
#     #
#     #             # 找到 cfg 中对应的 symbol（int），保证目录命名与外部一致
#     #             cfg_symbol = self.symbol_str_to_cfg_symbol.get(sid_str)
#     #             if cfg_symbol is None:
#     #                 # 理论上不会走到这里，因为上面已经用 symbol_str_set 限制过
#     #                 continue
#     #
#     #             # 用 arrow 的布尔 mask 在当前 batch 内做过滤（只对当前 batch 扫一遍）
#     #             mask = pc.equal(sym_arr, pa.scalar(sid_str))
#     #             sub_batch = batch.filter(mask)
#     #             if sub_batch.num_rows == 0:
#     #                 continue
#     #
#     #             # 获取 / 创建该 symbol 当日的 ParquetWriter
#     #             writer = writers.get(cfg_symbol)
#     #             if writer is None:
#     #                 out_dir = self.path_manager.symbol_dir(cfg_symbol, date)
#     #                 FileSystem.ensure_dir(out_dir)
#     #                 out_path = out_dir / f"{kind}.parquet"
#     #
#     #                 writer = pq.ParquetWriter(out_path, sub_batch.schema)
#     #                 writers[cfg_symbol] = writer
#     #
#     #             writer.write_batch(sub_batch)
#     #             row_counts[cfg_symbol] += sub_batch.num_rows
#     #
#     #     # 关闭所有 writer
#     #     for w in writers.values():
#     #         w.close()
#     #
#     #     logs.info(
#     #         f"[SymbolRouter] 完成拆分: {parquet_path} | "
#     #         f"batches={total_batches}, rows={total_rows}, "
#     #         f"symbols={len(writers)}"
#     #     )
#     #
#     #     # 如需记录元信息，可以在这里用 RouterMetadata 做统计
#     #     # 例如：self.meta.record_output(parquet_path, row_counts)
