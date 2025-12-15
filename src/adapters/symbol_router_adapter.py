from __future__ import annotations

from pathlib import Path
from typing import Iterable

from src.adapters.base_adapter import BaseAdapter
from src.engines.symbol_router_engine import SymbolRouterEngine


class SymbolRouterAdapter(BaseAdapter):
    """
    Adapter = 策略 + 编排

    - 决定处理哪些文件
    - 决定哪些 symbol
    - 调用 Engine 执行
    - 在此层记录 leaf timer
    """

    def __init__(
            self,
            *,
            engine: SymbolRouterEngine,
            symbols: Iterable[str],
            inst=None,
    ):
        super().__init__(inst)
        self.engine = engine
        self.symbols = {f"{int(s):06d}" for s in symbols}

    def split(
            self,
            *,
            date: str,
            parquet_files: list[Path],
            symbol_dir: Path,
    ) -> None:
        for p in parquet_files:
            kind = self._infer_kind(p.name)
            if kind is None:
                continue

            # leaf timer（accounting 单元）
            with self.timer():
                self.engine.route_file(
                    date=date,
                    kind=kind,
                    parquet_path=p,
                    symbol_dir=symbol_dir,
                    symbols=self.symbols,
                )

    @staticmethod
    def _infer_kind(name: str) -> str | None:
        n = name.lower()
        if "order" in n:
            return "Order"
        if "trade" in n:
            return "Trade"
        return None
