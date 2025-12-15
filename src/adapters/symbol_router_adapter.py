#!filepath: src/adapters/symbol_router_adapter.py
from __future__ import annotations
from pathlib import Path

from src import FileSystem
from src.adapters.base_adapter import BaseAdapter
from src.engines.symbol_router_engine import SymbolRouterEngine


class SymbolRouterAdapter(BaseAdapter):
    """
    Adapter = 编排 + 策略

    - 决定哪些文件
    - 决定哪些 symbol
    - 调用 Engine 执行
    """

    def __init__(
            self,
            *,
            engine: SymbolRouterEngine,
            symbols: list[str],
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

            # leaf timer（可选）
            with self.timer(f"symbol_router_route_{kind}"):
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
