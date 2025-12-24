from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from src.engines.normalize_engine import NormalizeEngine
from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.meta.meta import BaseMeta, MetaResult
from src.meta.symbol_accessor import SymbolAccessor  # 按你项目实际路径调整
from src.pipeline.context import EngineContext


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _write_parquet(path: Path, table: pa.Table) -> Path:
    pq.write_table(table, path)
    return path


def _build_symbol_slice_index(table: pa.Table) -> Dict[str, Tuple[int, int]]:
    """
    生成 (symbol -> (start, length))，要求 table 已按 symbol 排序
    """
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
def test_symbol_accessor_normalize_orderbook_snapshot_e2e(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    全链路：
      exchange parquet (sh_order + sh_trade)
        -> NormalizeEngine.execute() 各自产出 canonical parquet
        -> 合并两份 canonical（同 schema）并按 (symbol, ts) 排序
        -> BaseMeta.commit() 生成 normalize manifest（带 symbol slice index）
        -> SymbolAccessor.from_manifest() + accessor.get(symbol)
        -> 写出该 symbol 的 canonical parquet
        -> OrderBookRebuildEngine.offline -> snapshot.parquet
        -> 断言 snapshot 正确

    断言覆盖：
      - parse_events_arrow 的字段严格性（TradeTime 必须存在）
      - Normalize 的过滤与输出 schema
      - Meta manifest + SymbolAccessor slice 正确
      - OrderBook 重建（ADD/CANCEL/TRADE）最终 snapshot 正确
    """

    # ------------------------------------------------------------
    # 0) 为了让 ts 断言稳定：把 base_us 固定为 0（只验证 offset_us 部分）
    #    这样 ts == tick_to_offset_us(TickTime)
    # ------------------------------------------------------------
    import src.engines.parser_engine as parser_engine
    monkeypatch.setattr(parser_engine, "trade_time_to_base_us", lambda _: 0)

    # ------------------------------------------------------------
    # 1) 构造交易所级输入 parquet
    #    注意：必须包含 TradeTime 列（即便 registry 的 time_field 不是 TradeTime）
    # ------------------------------------------------------------
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "normalized"
    meta_dir = tmp_path / "meta"
    in_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    # 选一个 A 股 symbol（通过 filter_a_share_arrow）
    sym = "600000"

    # -------- sh_order.parquet -> 产出 ADD/CANCEL
    # TickTime 编码必须符合你 tick_to_offset_us 的假设：
    #   t = HH*1_000_000 + MM*10_000 + SS*100 + mmm
    sh_order = pa.table(
        {
            "SecurityID": [sym, sym],
            "TradeTime": ["2015-01-01 00:00:00", "2015-01-01 00:00:00"],  # 必须存在
            "TickTime": [9_300_000, 9_300_500],  # 09:30:00.000 / 09:30:05.000
            "TickType": ["A", "D"],              # A=ADD, D=CANCEL
            "Price": [10.0, 10.0],
            "Volume": [100, 100],
            "Side": ["1", "1"],                  # 1=B
            "SubSeq": [1, 1],                    # order_id
        }
    )
    sh_order_path = _write_parquet(in_dir / "sh_order.parquet", sh_order)

    # -------- sh_trade.parquet -> 产出 TRADE（需要 BuyNo/SellNo）
    sh_trade = pa.table(
        {
            "SecurityID": [sym],
            "TradeTime": ["2015-01-01 00:00:00"],  # 必须存在
            "TickTime": [9_300_200],               # 09:30:02.000
            "TickType": ["T"],                     # T=TRADE
            "Price": [10.0],
            "Volume": [40],                        # 成交 40
            "Side": ["1"],                         # 1=B（按你 registry）
            "SubSeq": [1],                         # trade 用同一个 order_id 扣减
            "BuyNo": [10],
            "SellNo": [20],
        }
    )
    sh_trade_path = _write_parquet(in_dir / "sh_trade.parquet", sh_trade)

    # ------------------------------------------------------------
    # 2) NormalizeEngine：分别执行 order / trade
    # ------------------------------------------------------------
    engine = NormalizeEngine()

    r_order = engine.execute(sh_order_path, out_dir)
    r_trade = engine.execute(sh_trade_path, out_dir)

    assert r_order.output_file.exists()
    assert r_trade.output_file.exists()

    t_order = pq.read_table(r_order.output_file)
    t_trade = pq.read_table(r_trade.output_file)

    # Normalize 输出必须包含你内部 schema字段（至少这些）
    for t in (t_order, t_trade):
        for col in ["symbol", "ts", "event", "order_id", "side", "price", "volume", "buy_no", "sell_no"]:
            assert col in t.schema.names

    # ------------------------------------------------------------
    # 3) 合并两份 canonical，并按 (symbol, ts) 排序
    #    这一步相当于你 pipeline 中“把 order+trade 统一成一条事件流”
    # ------------------------------------------------------------
    merged = pa.concat_tables([t_order, t_trade], promote_options="default")

    sort_idx = pc.sort_indices(
        merged,
        sort_keys=[
            ("symbol", "ascending"),
            ("ts", "ascending"),
        ],
    )
    merged = merged.take(sort_idx)

    merged_path = out_dir / "sh_all.parquet"
    pq.write_table(merged, merged_path)

    # ------------------------------------------------------------
    # 4) 为 merged 生成 normalize manifest（BaseMeta + MetaResult）
    # ------------------------------------------------------------
    index = _build_symbol_slice_index(merged)

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(
        MetaResult(
            input_file=merged_path,      # 这里把 merged 当作 normalize 的输入来源
            output_file=merged_path,     # 输出就是 merged 自己（manifest 只负责“唯一真相”）
            rows=merged.num_rows,
            index=index,
        )
    )

    manifest_path = meta.manifest_path(merged_path.stem)  # commit 用 output_file.stem
    assert manifest_path.exists()

    # ------------------------------------------------------------
    # 5) SymbolAccessor：从 manifest 构建 + slice 取该 symbol
    # ------------------------------------------------------------
    accessor = SymbolAccessor.from_manifest(manifest_path)
    sym_table = accessor.get(sym)

    assert sym_table.num_rows == merged.num_rows  # 这里只有一个 symbol
    assert sym_table.column("symbol").to_pylist() == [sym] * sym_table.num_rows

    # 写出该 symbol 的 canonical parquet，喂给 OrderBookRebuildEngine
    sym_canonical_path = tmp_path / f"{sym}.canonical.parquet"
    pq.write_table(sym_table, sym_canonical_path)

    # ------------------------------------------------------------
    # 6) OrderBookRebuildEngine：offline 重建 snapshot
    # ------------------------------------------------------------
    snapshot_path = tmp_path / "orderbook.parquet"
    ob = OrderBookRebuildEngine(record_events=False)

    ob.execute(
        EngineContext(
            mode="offline",
            input_path=sym_canonical_path,
            output_path=snapshot_path,
        )
    )

    assert snapshot_path.exists()
    snap = pq.read_table(snapshot_path).to_pylist()

    # ------------------------------------------------------------
    # 7) 断言 snapshot 正确
    #
    # 输入事件语义：
    #   ts=9:30:00 ADD  oid=1 B 10.0 vol=100
    #   ts=9:30:02 TRADE oid=1 B 10.0 vol=40   -> 剩余 60
    #   ts=9:30:05 CANCEL oid=1                -> 撤单后盘口应为空
    #
    # 所以最终 snapshot 为空
    # ------------------------------------------------------------
    assert len(snap) == 0
