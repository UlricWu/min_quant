from __future__ import annotations

from pathlib import Path
from typing import Iterable

from src.adapters.aggregate_minute_bar_adapter import AggregateMinuteBarAdapter
from src import logs


class AggregateMinuteBarStep:
    """
    Step 职责（严格受限）：
    - 遍历 symbol
    - 组织输入 / 输出路径
    - 控制是否跳过已存在结果

    Step 不做：
    - 任何 DataFrame 操作
    - 任何业务 / 聚合 / 时间逻辑
    """

    def __init__(
        self,
        adapter: AggregateMinuteBarAdapter,
        truth_trade_root: Path,
        skip_if_exists: bool = True,
    ) -> None:
        self.adapter = adapter
        self.truth_trade_root = truth_trade_root
        self.skip_if_exists = skip_if_exists

    def run(
        self,
        symbols: Iterable[str],
        date: str,
    ) -> None:
        """
        Parameters
        ----------
        symbols : Iterable[str]
            Symbols to process
        date : str
            Trading date (YYYY-MM-DD)
        """
        for symbol in symbols:
            trade_path = (
                self.truth_trade_root
                / symbol
                / f"date={date}.parquet"
            )

            if not trade_path.exists():
                logs.warning(
                    f"[MinuteBarStep] trade not found, skip: {trade_path}"
                )
                continue

            out_path = (
                self.adapter.out_root
                / symbol
                / f"date={date}.parquet"
            )

            if self.skip_if_exists and out_path.exists():
                logs.info(
                    f"[MinuteBarStep] exists, skip: {out_path}"
                )
                continue

            self.adapter.run(
                trade_path=trade_path,
                symbol=symbol,
                date=date,
            )
