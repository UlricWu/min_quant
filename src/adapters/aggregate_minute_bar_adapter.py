from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.aggregate_minute_bar_engine import AggregateMinuteBarEngine
from src import logs


class AggregateMinuteBarAdapter:
    """
    Adapter 职责（严格受限）：
    - 从 parquet 读取 truth trade
    - 调用 AggregateMinuteBarEngine
    - 将结果写回 parquet

    Adapter 不做：
    - 时间对齐
    - 数据修复
    - 聚合逻辑
    - 金融假设
    """

    def __init__(
        self,
        engine: AggregateMinuteBarEngine,
        out_root: Path,
        compression: str = "zstd",
        compression_level: int = 3,
    ) -> None:
        self.engine = engine
        self.out_root = out_root
        self.compression = compression
        self.compression_level = compression_level

    def run(
        self,
        trade_path: Path,
        symbol: str,
        date: str,
    ) -> Path:
        """
        Parameters
        ----------
        trade_path : Path
            Path to truth trade parquet (single symbol, single date)
        symbol : str
            Symbol identifier (used only for output path)
        date : str
            Trading date (YYYY-MM-DD, used only for output path)

        Returns
        -------
        Path
            Output parquet path
        """
        if not trade_path.exists():
            raise FileNotFoundError(trade_path)

        logs.info(f"[MinuteBarAdapter] read trade: {trade_path}")

        trade_df = pq.read_table(trade_path).to_pandas()

        # 调用纯 Engine（唯一计算发生点）
        bar_df = self.engine.run(trade_df)

        out_path = (
            self.out_root
            / symbol
            / f"date={date}.parquet"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(bar_df, preserve_index=False)

        pq.write_table(
            table,
            out_path,
            compression=self.compression,
            compression_level=self.compression_level,
        )

        logs.info(f"[MinuteBarAdapter] write bar: {out_path}")

        return out_path
