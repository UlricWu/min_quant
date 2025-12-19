#!filepath: src/dataloader/pipeline/steps/csv_convert_step.py
from __future__ import annotations

from pathlib import Path

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src import logs


class CsvConvertStep(BasePipelineStep):
    """
    CSV→Parquet 转换 Step：
    - 遍历 raw_dir 下所有 *.7z
    - 根据文件名判断 file_type
    - 判断目标 parquet 是否存在（skip 策略）
    - 调用 CsvConvertAdapter 执行转换
    """

    def __init__(self, sh_adapter, sz_adapter, inst=None):
        super().__init__(inst)
        self.sh_adapter = sh_adapter
        self.sz_adapter = sz_adapter

    # ------------------------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        raw_dir = ctx.raw_dir
        parquet_dir = ctx.parquet_dir

        with self.timed():
            for zfile in raw_dir.glob("*.7z"):
                file_type = self._detect_type(zfile.name)

                out_files = self._build_out_files(zfile, parquet_dir, file_type)

                if self._all_exist(out_files):
                    logs.info(f"[CsvConvertStep] skip {zfile.name}")
                    continue

                with self.inst.timer(file_type):
                    if file_type == "SH_MIXED":
                        self.sh_adapter.convert(zfile, out_files)
                    else:
                        self.sz_adapter.convert(zfile, out_files)

        return ctx

    @staticmethod
    def _build_out_files(zfile: Path, parquet_dir: Path, file_type: str) -> dict[str, Path]:
        if file_type == "SH_MIXED":
            return {
                "order": parquet_dir / "SH_Order.parquet",
                "trade": parquet_dir / "SH_Trade.parquet",
            }

        stem = zfile.stem.replace(".csv", "")
        return {
            "default": parquet_dir / f"{stem}.parquet"
        }

    @staticmethod
    def _all_exist(out_files: dict[str, Path]) -> bool:
        return all(p.exists() for p in out_files.values())

    # ------------------------------------------------------------------
    @staticmethod
    def _detect_type(filename: str) -> str:
        """
        根据文件名约定识别 file_type：
            SH_Stock_OrderTrade.csv.7z → SH_MIXED
            SH_Order.csv.7z           → SH_ORDER
            SH_Trade.csv.7z           → SH_TRADE
            SZ_Order.csv.7z           → SZ_ORDER
            SZ_Trade.csv.7z           → SZ_TRADE
        """
        lower = filename.lower()

        if lower.startswith("sh_stock_ordertrade"):
            return "SH_MIXED"

        if lower.startswith("sh_order"):
            return "SH_ORDER"
        if lower.startswith("sh_trade"):
            return "SH_TRADE"

        if lower.startswith("sz_order"):
            return "SZ_ORDER"
        if lower.startswith("sz_trade"):
            return "SZ_TRADE"

        raise RuntimeError(f"无法识别文件类型: {filename}")

    # ------------------------------------------------------------------

    def detect_exist(self, zfile: Path, parquet_dir: Path, file_type: str) -> bool:
        """
        Step 层的 skip 策略（Engine / Adapter 都不管）：
        - SH_MIXED 要求 SH_Order + SH_Trade 都存在才算完成
        - 其他 file_type 只检查单个 parquet 是否存在
        """
        if file_type.upper() == "SH_MIXED":
            order_path = parquet_dir / "SH_Order.parquet"
            trade_path = parquet_dir / "SH_Trade.parquet"
            return order_path.exists() and trade_path.exists()

        stem = zfile.stem.replace(".csv", "")
        target = parquet_dir / f"{stem}.parquet"
        return target.exists()
