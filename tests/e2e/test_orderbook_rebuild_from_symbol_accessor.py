from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from src.engines.normalize_engine import NormalizeEngine
from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.engines.parser_engine import parse_events_arrow
from src.meta.meta import BaseMeta, MetaResult
from src.meta.symbol_accessor import SymbolAccessor
from src.pipeline.context import EngineContext


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _write_parquet(path: Path, table: pa.Table) -> Path:
    pq.write_table(table, path)
    return path


def _build_symbol_slice_index(table: pa.Table) -> Dict[str, Tuple[int, int]]:
    if table.num_rows == 0:
        return {}

    symbols = table["symbol"].to_pylist()
    index: Dict[str, Tuple[int, int]] = {}

    start = 0
    cur = symbols[0]
    for i in range(1, len(symbols)):
        if symbols[i] != cur:
            index[cur] = (start, i - start)
            cur = symbols[i]
            start = i
    index[cur] = (start, len(symbols) - start)
    return index


# ------------------------------------------------------------
# E2E
# ------------------------------------------------------------
def test_symbol_accessor_normalize_orderbook_snapshot_e2e(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Normalize(v2) + Meta + SymbolAccessor + OrderBookRebuild 的完整 E2E
    """

    # ------------------------------------------------------------
    # 0) 固定 base_us，保证 ts 可预测
    # ------------------------------------------------------------
    import src.engines.parser_engine as parser_engine
    monkeypatch.setattr(parser_engine, "trade_time_to_base_us", lambda _: 0)

    # ------------------------------------------------------------
    # 1) 构造交易所级输入 parquet
    # ------------------------------------------------------------
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "out"
    meta_dir = tmp_path / "meta"
    in_dir.mkdir()
    out_dir.mkdir()
    meta_dir.mkdir()

    sym = "600000"

    sh_order = pa.table(
        {
            "SecurityID": [sym, sym],
            "TradeTime": ["2015-01-01 00:00:00"] * 2,
            "TickTime": [9_300_000, 9_300_500],
            "TickType": ["A", "D"],
            "Price": [10.0, 10.0],
            "Volume": [100, 100],
            "Side": ["1", "1"],
            "SubSeq": [1, 1],
        }
    )
    sh_trade = pa.table(
        {
            "SecurityID": [sym],
            "TradeTime": ["2015-01-01 00:00:00"],
            "TickTime": [9_300_200],
            "TickType": ["T"],
            "Price": [10.0],
            "Volume": [40],
            "Side": ["1"],
            "SubSeq": [1],
            "BuyNo": [10],
            "SellNo": [20],
        }
    )

    order_path = _write_parquet(in_dir / "sh_order.parquet", sh_order)
    trade_path = _write_parquet(in_dir / "sh_trade.parquet", sh_trade)

    # ------------------------------------------------------------
    # 2) 显式 parse（这一步原来是 NormalizeEngine 内部）
    # ------------------------------------------------------------
    t_order = parse_events_arrow(
        pq.read_table(order_path),
        exchange="sh",
        kind="order",
    )
    t_trade = parse_events_arrow(
        pq.read_table(trade_path),
        exchange="sh",
        kind="trade",
    )

    # ------------------------------------------------------------
    # 3) NormalizeEngine v2（纯 Arrow）
    # ------------------------------------------------------------
    engine = NormalizeEngine()

    norm = engine.execute([t_order, t_trade])
    canonical = norm.canonical

    canonical_path = out_dir / "sh_all.parquet"
    pq.write_table(canonical, canonical_path)

    # ------------------------------------------------------------
    # 4) Meta（manifest 唯一真相）
    # ------------------------------------------------------------
    index = _build_symbol_slice_index(canonical)

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(
        MetaResult(
            input_file=canonical_path,
            output_file=canonical_path,
            rows=canonical.num_rows,
            index=index,
        )
    )

    manifest_path = meta.manifest_path(canonical_path.stem)
    assert manifest_path.exists()

    # ------------------------------------------------------------
    # 5) SymbolAccessor
    # ------------------------------------------------------------
    accessor = SymbolAccessor.from_manifest(manifest_path)
    sym_table = accessor.get(sym)

    assert sym_table.num_rows == canonical.num_rows
    assert set(sym_table["symbol"].to_pylist()) == {sym}

    sym_path = tmp_path / f"{sym}.canonical.parquet"
    pq.write_table(sym_table, sym_path)

    # ------------------------------------------------------------
    # 6) OrderBookRebuildEngine
    # ------------------------------------------------------------
    snapshot_path = tmp_path / "orderbook.parquet"
    ob = OrderBookRebuildEngine(record_events=False)

    ob.execute(
        EngineContext(
            mode="offline",
            input_path=sym_path,
            output_path=snapshot_path,
        )
    )

    assert snapshot_path.exists()
    snap = pq.read_table(snapshot_path).to_pylist()

    # ------------------------------------------------------------
    # 7) 断言 snapshot
    #
    # ADD 100 -> TRADE 40 -> CANCEL -> 盘口为空
    # ------------------------------------------------------------
    assert len(snap) == 0
