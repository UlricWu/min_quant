# #!filepath: src/l2/offline/offline_batch_runner.py
#
# from pathlib import Path
# import pyarrow.dataset as ds
#
# from src import logs
# from src.utils.datetime_utils import DateTimeUtils as dt
# from src.l2.common.event_mapper import EventMapper
#
#
# class OfflineBatchRunner:
#     """
#     读取 symbol 级 parquet（Order / Trade）
#     → Arrow RecordBatch
#     → EventMapper(map_batch)
#     → InternalEvent Batch
#     """
#
#     def __init__(self, path_manager):
#         self.pm = path_manager
#
#     # ------------------------------------------------
#     def run_date(self, symbol: int | str, date: str, kind: str):
#         path = (self.pm.order_path(symbol, date)
#                 if kind == "order"
#                 else self.pm.trade_path(symbol, date))
#
#         if not path.exists():
#             logs.warning(f"[OfflineBatchRunner] {path} 不存在 → 跳过")
#             return
#
#         # 推断交易所：688/60=SH, 00/30=SZ
#         exch = self._infer_exchange(symbol)
#
#         mapper = EventMapper(exch, kind)
#         dataset = ds.dataset(path, format="parquet")
#
#         for batch in dataset.to_batches():
#             trade_date = date  # 你的 date 已经是 YYYY-MM-DD or YYYYMMDD
#             event_batch = mapper.map_batch(batch, trade_date)
#             yield event_batch
#
#     # ------------------------------------------------
#     @staticmethod
#     def _infer_exchange(symbol) -> int:
#         s = str(symbol).zfill(6)
#         if s.startswith(("60", "688")):
#             return 1
#         if s.startswith(("00", "30")):
#             return 2
#         raise ValueError(f"[OfflineBatchRunner] 无法推断交易所: {symbol}")
#!filepath: src/l2/offline/offline_batch_runner.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, List

import pandas as pd

from src import logs
from src.utils.filesystem import FileSystem
from src.utils.path import PathManager
from src.l2.common.event_parser import parse_events
from src.l2.offline.trade_enrich_engine import TradeEnrichEngine, TradeEnrichConfig


@dataclass
class OfflineBatchRunnerConfig:
    """
    Offline 逐笔特征生成配置
    """
    burst_window_ms: int = 5


class OfflineBatchRunner:
    """
    OfflineBatchRunner
    ------------------
    输入：SymbolRouter 已生成的 symbol 级 Trade.parquet:
        data/symbol/<symbol>/<date>/Trade.parquet

    流程：
        1) 读 Trade.parquet（单 symbol，行数 ~10^5 量级，可用 pandas）
        2) parse_events(df, kind="trade") → 统一事件结构（ts, event, price, volume, ...）
        3) TradeEnrichEngine.enrich_dataframe(df) → 增加 burst_id/is_price_impact
        4) 写出 Trade_Enriched.parquet

    设计：
        - 不关心 CSV / 7z 等上游逻辑
        - 不关心 symbol list 从哪里来（由调用方注入）
        - 仅依赖 PathManager（路径策略） + parse_events（事件解析） + TradeEnrichEngine（状态机）
    """

    def __init__(
        self,
        path_manager: PathManager,
        symbols: Iterable[int | str],
        cfg: Optional[OfflineBatchRunnerConfig] = None,
    ) -> None:
        self.pm = path_manager
        self.symbols: List[int] = [int(s) for s in symbols]
        self.cfg = cfg or OfflineBatchRunnerConfig()
        self.enricher = TradeEnrichEngine(
            TradeEnrichConfig(burst_window_ms=self.cfg.burst_window_ms)
        )

    # ------------------- 路径封装 -------------------
    def _trade_path(self, symbol: int, date: str) -> Path:
        # 假设 PathManager 有 trade_path；若没有可改成 symbol_dir + "Trade.parquet"
        if hasattr(self.pm, "trade_path"):
            return self.pm.trade_path(symbol, date)
        return self.pm.symbol_dir(symbol, date) / "Trade.parquet"

    def _enriched_path(self, symbol: int, date: str) -> Path:
        if hasattr(self.pm, "enriched_trade_path"):
            return self.pm.enriched_trade_path(symbol, date)
        return self.pm.symbol_dir(symbol, date) / "Trade_Enriched.parquet"

    # ------------------- 单 symbol/date -------------------
    def run_symbol_date(self, symbol: int, date: str) -> None:
        trade_path = self._trade_path(symbol, date)
        enriched_path = self._enriched_path(symbol, date)

        if enriched_path.exists():
            logs.info(
                f"[OfflineBatchRunner] {symbol} {date} Trade_Enriched 已存在 → 跳过"
            )
            return

        if not trade_path.exists():
            logs.warning(
                f"[OfflineBatchRunner] {symbol} {date} Trade.parquet 不存在 → 跳过"
            )
            return

        logs.info(f"[OfflineBatchRunner] 读取 Trade: {trade_path}")
        df_raw = pd.read_parquet(trade_path)

        if df_raw.empty:
            logs.warning(
                f"[OfflineBatchRunner] {symbol} {date} Trade.parquet 为空 → 跳过"
            )
            return

        # 事件解析（统一成 InternalEvent 结构）
        df_events = parse_events(df_raw, kind="trade")

        # 逐笔增强（burst + impact）
        df_enriched = self.enricher.enrich_dataframe(df_events)

        FileSystem.ensure_dir(enriched_path.parent)
        df_enriched.to_parquet(enriched_path, index=False)
        logs.info(
            f"[OfflineBatchRunner] {symbol} {date} enriched 输出: {enriched_path}"
        )

    # ------------------- 整个日期（多 symbol） -------------------
    def run_date(self, date: str, symbols: Optional[Iterable[int | str]] = None) -> None:
        if symbols is None:
            symbols_iter = self.symbols
        else:
            symbols_iter = [int(s) for s in symbols]

        logs.info(f"[OfflineBatchRunner] ==== Trade Enrich date={date} 开始 ====")

        for sym in symbols_iter:
            try:
                self.run_symbol_date(sym, date)
            except Exception as e:
                logs.exception(
                    f"[OfflineBatchRunner] 处理 symbol={sym}, date={date} 时出错: {e}"
                )

        logs.info(f"[OfflineBatchRunner] ==== Trade Enrich date={date} 完成 ====")
