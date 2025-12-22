from pathlib import Path
from src.pipeline.step import BasePipelineStep
from src.pipeline.pipeline import PipelineContext
from src.utils.logger import logs
from src.engines.convert_engine import ConvertEngine


class CsvConvertStep(BasePipelineStep):
    def __init__(self, engine: ConvertEngine, inst=None):
        super().__init__(inst)
        self.engine = engine

    def run(self, ctx: PipelineContext):
        input_dir = ctx.raw_dir
        out_dir = ctx.parquet_dir

        for zfile in input_dir.glob("*.7z"):
            out_files = self._build_out_files(zfile, out_dir)
            if self._all_exist(out_files):
                logs.warning(f"[CsvConvertStep] skip {zfile.name}")
                continue

            with self.inst.timer(f'Process_{zfile.name}'):
                logs.info(f'[CsvConvertStep] start converting {zfile.name}')
                self.engine.convert(zfile, out_files)

        return ctx

    def _detect_type(self, filename):
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

    def _build_out_files(self, zfile: Path, parquet_dir: Path) -> dict[str, Path]:
        file_type = self._detect_type(zfile.stem)
        if file_type == "SH_MIXED":
            return {
                "sh_order": parquet_dir / "sh_order.parquet",
                "sh_trade": parquet_dir / "sh_trade.parquet",
            }

        stem = zfile.stem.replace(".csv", "")
        return {
            stem.lower(): parquet_dir / f"{stem.lower()}.parquet"
        }

    @staticmethod
    def _all_exist(out_files: dict[str, Path]) -> bool:
        return all(p.exists() for p in out_files.values())
