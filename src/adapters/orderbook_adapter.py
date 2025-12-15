from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.adapters.base_adapter import BaseAdapter
from src.engines.orderbook_engine import (
    OrderBookRebuildEngine,
    InternalEvent,
)
from src.l2.common.event_normalizer import normalize_events
from src.l2.common.exchange_registry import EXCHANGE_REGISTRY
from src.l2.common.event_parser import infer_exchange_id
from src.utils.filesystem import FileSystem
from src import logs


class OrderBookRebuildAdapter(BaseAdapter):
    """
    Adapter 职责：
    - 读 parquet
    - normalize_events
    - 调用 engines
    - 写 snapshot parquet
    """

    def __init__(
        self,
        engine: OrderBookRebuildEngine,
        *,
        snapshot_interval_ms: int | None = None,
        inst=None,
    ):
        super().__init__(inst)
        self.engine = engine
        self.snapshot_interval_ms = snapshot_interval_ms

    def rebuild_symbol_day(
        self,
        *,
        symbol: str,
        date: str,
        order_path: Path,
        trade_path: Path,
        out_path: Path,
    ) -> None:

        logs.info(f"[OrderBook] rebuild symbol={symbol} date={date}")

        dfs = []
        if order_path.exists():
            dfs.append(("order", pd.read_parquet(order_path)))
        if trade_path.exists():
            dfs.append(("trade", pd.read_parquet(trade_path)))

        if not dfs:
            logs.warning(f"[OrderBook] no input parquet for {symbol} {date}")
            return

        exchange_id = infer_exchange_id(symbol)

        events: list[InternalEvent] = []

        for kind, df in dfs:
            definition = EXCHANGE_REGISTRY[exchange_id][kind]
            norm = normalize_events(
                df,
                definition=definition,
                trade_date=date,
            )

            for row in norm.itertuples(index=False):
                events.append(InternalEvent(**row._asdict()))

        events.sort(key=lambda e: e.ts)

        FileSystem.ensure_dir(out_path)
        snapshots = []

        with self.timer():
            for snap in self.engine.rebuild(
                events,
                snapshot_interval_ms=self.snapshot_interval_ms,
            ):
                snapshots.append(
                    {
                        "ts": snap.ts,
                        "bids": snap.bids,
                        "asks": snap.asks,
                    }
                )

        if snapshots:
            df_out = pd.DataFrame(snapshots)
            df_out.to_parquet(out_path / "OrderBook.parquet", index=False)
