# #!filepath: tests/engines/test_normalize_engine.py
# import pandas as pd
# import pytest
# from pathlib import Path
#
# from src.engines.normalize_engine import NormalizeEngine
# from src.engines.context import EngineContext
# from src.l2.common.normalized_event import NormalizedEvent
#
#
# # =========================================================
# # Fixtures
# # =========================================================
# @pytest.fixture
# def tmp_paths(tmp_path: Path):
#     in_dir = tmp_path / "in"
#     out_dir = tmp_path / "out" / "Events.parquet"
#     in_dir.mkdir(parents=True)
#     return in_dir, out_dir
#
#
# @pytest.fixture
# def order_df():
#     return pd.DataFrame({
#         "TradeTime": ["2025-11-07 09:00:00"] * 2,
#         "TickTime": ["091500000", "091501000"],
#         "TickType": ["A", "D"],
#         "Side": ["1", "2"],
#         "SubSeq": [1, 2],
#         "Price": [10.0, 10.1],
#         "Volume": [100, 200],
#         "BuyNo": [0, 0],
#         "SellNo": [0, 0],
#         "SecurityID": ["600001", "600001"],
#     })
#
#
# @pytest.fixture
# def trade_df():
#     return pd.DataFrame({
#         "TradeTime": ["2025-11-07 09:00:00"],
#         "TickTime": ["091502000"],
#         "TickType": ["T"],
#         "Side": ["1"],
#         "SubSeq": [3],
#         "Price": [10.2],
#         "Volume": [300],
#         "BuyNo": [11],
#         "SellNo": [22],
#         "SecurityID": ["600001"],
#     })
#
#
# # =========================================================
# # Tests: Offline
# # =========================================================
# def test_normalize_offline_basic(tmp_paths, order_df, trade_df):
#     in_dir, out_path = tmp_paths
#
#     order_df.to_parquet(in_dir / "Order.parquet")
#     trade_df.to_parquet(in_dir / "Trade.parquet")
#
#     ctx = EngineContext(
#         mode="offline",
#         symbol="600001",
#         date="2025-11-07",
#         input_path=in_dir,
#         output_path=out_path,
#     )
#
#     engine = NormalizeEngine()
#     events = engine.execute(ctx)
#
#     assert len(events) == 3
#     assert all(isinstance(e, NormalizedEvent) for e in events)
#
#     # ts 必须是 int
#     assert all(isinstance(e.ts, int) for e in events)
#
#     # 时间必须排序
#     ts_list = [e.ts for e in events]
#     assert ts_list == sorted(ts_list)
#
#     # parquet 必须写出
#     assert out_path.exists()
#
#     out_df = pd.read_parquet(out_path)
#     assert len(out_df) == 3
#     assert set(out_df["event"]) == {"ADD", "CANCEL", "TRADE"}
#
#
# def test_normalize_offline_drop_invalid_event(tmp_paths, order_df):
#     in_dir, out_path = tmp_paths
#
#     order_df = order_df.copy()
#     order_df.loc[0, "TickType"] = "X"  # 非法事件
#
#     order_df.to_parquet(in_dir / "Order.parquet")
#
#     ctx = EngineContext(
#         mode="offline",
#         symbol="600001",
#         date="2025-11-07",
#         input_path=in_dir,
#         output_path=out_path,
#     )
#
#     engine = NormalizeEngine()
#     events = engine.execute(ctx)
#
#     # 只剩 1 条合法事件
#     assert len(events) == 1
#     assert events[0].event in {"ADD", "CANCEL", "TRADE"}
#
#
# def test_normalize_offline_empty_input(tmp_paths):
#     in_dir, out_path = tmp_paths
#
#     pd.DataFrame().to_parquet(in_dir / "Order.parquet")
#
#     ctx = EngineContext(
#         mode="offline",
#         symbol="600001",
#         date="2025-11-07",
#         input_path=in_dir,
#         output_path=out_path,
#     )
#
#     engine = NormalizeEngine()
#     events = engine.execute(ctx)
#
#     assert events == []
#     assert not out_path.exists()
#
#
# # =========================================================
# # Tests: Realtime
# # =========================================================
# # def test_normalize_realtime_one_event(order_df):
# #     raw = order_df.iloc[0].to_dict()
# #
# #     ctx = EngineContext(
# #         mode="realtime",
# #         symbol="600001",
# #         date="2025-11-07",
# #         event=raw,
# #     )
# #
# #     engine = NormalizeEngine()
# #     events = engine.execute(ctx)
# #
# #     assert len(events) == 1
# #     ev = events[0]
# #
# #     assert isinstance(ev, NormalizedEvent)
# #     assert isinstance(ev.ts, int)
# #     assert ev.event in {"ADD", "CANCEL", "TRADE"}
#
#
# # def test_normalize_realtime_empty_fail():
# #     ctx = EngineContext(
# #         mode="realtime",
# #         symbol="600001",
# #         date="2025-11-07",
# #         event={},
# #     )
# #
# #     engine = NormalizeEngine()
# #     with pytest.raises(ValueError):
# #         engine.execute(ctx)
