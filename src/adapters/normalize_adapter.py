#!filepath: src/adapters/normalize_adapter.py
from __future__ import annotations

from pathlib import Path

from src.adapters.base_adapter import BaseAdapter
from src.engines.context import EngineContext
from src.engines.normalize_engine import NormalizeEngine
from src import logs


class NormalizeAdapter(BaseAdapter):
    """
    Normalize Adapter（最终版）

    职责：
    - 遍历 symbol
    - 构造 EngineContext
    - 调用 engine.execute(ctx)
    """

    def __init__(
            self,
            engine: NormalizeEngine,
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
            if not sym_dir.exists():
                logs.warning(f'sym_dir={sym_dir} does not exist')
                continue

            output_path = sym_dir / "Events.parquet"

            if output_path.exists():
                logs.info(f"[Normalize] Events 已存在 → skip {sym}")
                continue
            ctx = EngineContext(
                mode="offline",
                symbol=sym,
                date=date,
                input_path=sym_dir,  # 👈 目录
                output_path=output_path,
            )

            with self.timer("normalize_symbol"):
                self.engine.execute(ctx)
