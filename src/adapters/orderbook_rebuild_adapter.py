#!filepath: src/adapters/orderbook_rebuild_adapter.py
from __future__ import annotations

from pathlib import Path

from src.adapters.base_adapter import BaseAdapter
from src.engines.context import EngineContext
from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src import logs


class OrderBookRebuildAdapter(BaseAdapter):
    """
    OrderBook Rebuild Adapter（最终版）

    职责：
    - 遍历 symbol 目录
    - 构造 EngineContext
    - 调用 engine.execute(ctx)

    不关心：
    - 事件如何 normalize
    - OrderBook 内部逻辑
    """

    def __init__(
            self,
            engine: OrderBookRebuildEngine,
            *,
            symbols: list[str],
            inst=None,
    ):
        super().__init__(inst)
        self.engine = engine
        self.symbols = [str(s).zfill(6) for s in symbols]

    # --------------------------------------------------
    def run(
            self,
            *,
            date: str,
            symbol_root: Path,
    ) -> None:
        for sym in self.symbols:
            sym_dir = symbol_root / sym / date
            event_path = sym_dir / "Events.parquet"
            snapshot_path = sym_dir / "Snapshot.parquet"

            if not event_path.exists():
                logs.warning(f"[OrderBook] {event_path} 不存在，skip symbol={sym}")
                continue

            if snapshot_path.exists():
                logs.info(f"[OrderBook] snapshot 已存在 → skip symbol={sym}")
                continue

            ctx = EngineContext(
                mode="offline",
                symbol=sym,
                date=date,
                input_path=event_path,
                output_path=snapshot_path,
                emit_snapshot=True,
            )

            # Adapter 级 timer（细粒度，不进 timeline）
            with self.timer("orderbook_rebuild_symbol"):
                self.engine.execute(ctx)
