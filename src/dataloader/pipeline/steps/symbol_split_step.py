# from __future__ import annotations
#
# from pathlib import Path
#
# from src.dataloader.pipeline.step import BasePipelineStep
# from src.dataloader.pipeline.context import PipelineContext
# from src.adapters.symbol_router_adapter import SymbolRouterAdapter
# from src import logs
#
#
# class SymbolSplitStep(BasePipelineStep):
#     """
#     SymbolSplitStep（最终版）
#
#     输入：
#         ctx.normalize_dir / Events.parquet
#     输出：
#         ctx.symbol_dir/<symbol>/{order,trade}/date=YYYY-MM-DD.parquet
#     """
#     MANIFEST_NAME = "_SYMBOL_SPLIT_MANIFEST.json"
#
#     def __init__(
#         self,
#         adapter: SymbolRouterAdapter,
#         inst=None,
#         skip_if_exists: bool = True,
#     ) -> None:
#         super().__init__(inst)
#         self.adapter = adapter
#         self.skip_if_exists = skip_if_exists
#
#     def run(self, ctx: PipelineContext) -> PipelineContext:
#         normalize_dir: Path = ctx.canonical_dir
#         symbol_root: Path = ctx.symbol_dir
#         date: str = ctx.date
#
#         manifest_path = symbol_root / self.MANIFEST_NAME
#
#         # ------------------------------------------------------
#         # 1) 若 manifest 存在且校验通过 → skip
#         # ------------------------------------------------------
#         if manifest_path.exists():
#             manifest = self._load_manifest(manifest_path)
#             if self._is_complete(manifest, symbol_root):
#                 logs.info(f"[SymbolSplitStep] skip date={date} (manifest valid)")
#                 return ctx
#             else:
#                 logs.warning(
#                     f"[SymbolSplitStep] manifest invalid, re-run date={date}"
#                 )
#
#         parquet_files = sorted(normalize_dir.glob("*.parquet"))
#         if not parquet_files:
#             logs.warning(f"[SymbolSplitStep] no parquet in {normalize_dir}")
#             return ctx
#
#         with self.timed():
#             for parquet_path in parquet_files:
#                 file_type = self._infer_file_type(parquet_path.name)
#
#
#
#                 # Step 级 skip（只检查是否已有任意 symbol 输出）
#                 # if self.skip_if_exists:
#                 # if self._any_symbol_exists(
#                 #         symbol_dir, file_type, date
#                 # ):
#                 #     logs.info(
#                 #         f"[SymbolSplitStep] skip {parquet_path.name} "
#                 #         f"(symbol outputs exist)"
#                 #     )
#                 #     continue
#
#
#                 with self.inst.timer(parquet_path.name):
#                     self.adapter.run(
#                         parquet_path=parquet_path,
#                         out_root=symbol_dir,
#                         file_type=file_type,
#                         date=date,
#                     )
#
#         return ctx
#
#     @staticmethod
#     def _infer_file_type(name: str) -> str:
#         n = name.lower()
#         if "order" in n:
#             return "order"
#         if "trade" in n:
#             return "trade"
#         raise ValueError(f"cannot infer file_type from {name}")
#
#     @staticmethod
#     def _any_symbol_exists(
#         symbol_dir: Path,
#         file_type: str,
#         date: str,
#     ) -> bool:
#         if not symbol_dir.exists():
#             return False
#
#         for sym_dir in symbol_dir.iterdir():
#             p = sym_dir / file_type / f"date={}.parquet"
#             if p.exists():
#                 return True
#
#         return False
