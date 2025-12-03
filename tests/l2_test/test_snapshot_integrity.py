# #!filepath: tests/l2_test/test_snapshot_integrity.py
# import pyarrow.parquet as pq
# import pandas as pd
# import numpy as np
# from datetime import datetime
# from zoneinfo import ZoneInfo
#
# from src.l2.orderbook.orderbook_rebuilder import OrderBookRebuilder
# from src.utils.path import PathManager
#
#
# SH_TZ = ZoneInfo("Asia/Shanghai")
#
# # ----------------------------------------------------------------------
# # Helper: 获取 Snapshot DataFrame
# # ----------------------------------------------------------------------
# def load_snapshot(symbol: str, date: str):
#     path = PathManager().snapshot_dir(symbol, date)
#     assert path.exists(), f"Snapshot 不存在: {path}"
#     return pq.read_table(path).to_pandas()
#
#
# # ----------------------------------------------------------------------
# # 1. 基础字段存在
# # ----------------------------------------------------------------------
# def test_snapshot_fields_exist(symbol="001287", date="2025-11-07"):
#     df = load_snapshot(symbol, date)
#
#     assert "ts" in df.columns, "缺少 ts 字段"
#
#     # 必须至少有 1 档
#     assert any(col.startswith("BidPrice") for col in df.columns)
#     assert any(col.startswith("AskPrice") for col in df.columns)
#     assert any(col.startswith("BidVolume") for col in df.columns)
#     assert any(col.startswith("AskVolume") for col in df.columns)
#
#
# # ----------------------------------------------------------------------
# # 2. ts 必须严格递增
# # ----------------------------------------------------------------------
# def test_ts_is_monotonic(symbol="001287", date="2025-11-07"):
#     df = load_snapshot(symbol, date)
#     ts = df["ts"]
#
#     assert ts.is_monotonic_increasing, "Snapshot ts 必须严格递增（已排序）"
#     assert ts.dt.tz is not None, "ts 必须是有时区的 datetime"
#     # assert str(ts.dt.tz.iloc[0]) == "Asia/Shanghai"
#
#
# # ----------------------------------------------------------------------
# # 3. 盘口结构合法性（Bid 最大价格必须是 L1）
# # ----------------------------------------------------------------------
# def test_bid_ask_price_order(symbol="001287", date="2025-11-07"):
#     df = load_snapshot(symbol, date)
#
#     bid_cols = sorted([c for c in df.columns if c.startswith("BidPrice")],
#                       key=lambda x: int(x.replace("BidPrice", "")))
#
#     ask_cols = sorted([c for c in df.columns if c.startswith("AskPrice")],
#                       key=lambda x: int(x.replace("AskPrice", "")))
#
#     for _, row in df.iterrows():
#         bids = [row[c] for c in bid_cols if row[c] is not None]
#         asks = [row[c] for c in ask_cols if row[c] is not None]
#
#         # Bid 应降序
#         assert bids == sorted(bids, reverse=True), f"Bid 价格未降序：{bids}"
#
#         # Ask 应升序
#         assert asks == sorted(asks), f"Ask 价格未升序：{asks}"
#
#         # 买卖盘中不能出现负数
#         assert all(p >= 0 for p in bids + asks), "价格不能为负"
#
#         # 至少要有一档存在（完全空盘口基本不可能）
#         assert len(bids) + len(asks) > 0, "Bid/Ask 不可能同时为空"
#
#
# # ----------------------------------------------------------------------
# # 4. Volume 合法
# # ----------------------------------------------------------------------
# def test_volume_integrity(symbol="001287", date="2025-11-07"):
#     df = load_snapshot(symbol, date)
#
#     vol_cols = [
#         *[c for c in df.columns if c.startswith("BidVolume")],
#         *[c for c in df.columns if c.startswith("AskVolume")],
#     ]
#
#     for c in vol_cols:
#         assert (df[c] >= 0).all(), f"{c} 出现负数"
#         assert df[c].dtype in (int, float, np.int64, np.float64), f"{c} 类型异常"
#
#
# # ----------------------------------------------------------------------
# # 5. 连续事件：盘口应遵循吃单逻辑
# # ----------------------------------------------------------------------
# def test_market_microstructure(symbol="001287", date="2025-11-07"):
#     """
#     检查连续两帧，Bid1 和 Ask1 是否满足 Level-2 的微观结构特性：
#
#     若买单主动成交 → AskVolume1 应减少或 AskPrice1 上移
#     若卖单主动成交 → BidVolume1 应减少或 BidPrice1 下移
#     """
#
#     df = load_snapshot(symbol, date)
#
#     bid1 = df["BidPrice1"]
#     ask1 = df["AskPrice1"]
#
#     bidv1 = df["BidVolume1"]
#     askv1 = df["AskVolume1"]
#
#     for i in range(1, len(df)):
#         # 如果 AskVolume1 减少 → 买单主动打 Ask
#         if askv1[i] < askv1[i - 1]:
#             assert ask1[i] >= ask1[i - 1], "成交后 Ask 价格不应下降（卖盘不应倒挂）"
#
#         # 如果 BidVolume1 减少 → 卖单主动打 Bid
#         if bidv1[i] < bidv1[i - 1]:
#             assert bid1[i] <= bid1[i - 1], "成交后 Bid 价格不应上升（买盘不应倒挂）"
#
#
# # ----------------------------------------------------------------------
# # 6. Snapshot 不允许时间倒退
# # ----------------------------------------------------------------------
# def test_snapshot_no_time_backward(symbol="001287", date="2025-11-07"):
#     df = load_snapshot(symbol, date)
#     ts = df["ts"]
#
#     for i in range(1, len(ts)):
#         assert ts[i] >= ts[i - 1], f"时间倒退: {ts[i]} < {ts[i-1]}"
#
#
# # ----------------------------------------------------------------------
# # 7. Snapshot 行数合理（事件条数 ≥ Snapshot 条数）
# # ----------------------------------------------------------------------
# def test_snapshot_row_reasonable(symbol="001287", date="2025-11-07"):
#     df = load_snapshot(symbol, date)
#     assert len(df) > 0, "Snapshot 不能为空"
#     assert len(df) < 200_000, "Snapshot 行数过多，疑似循环错误"
#
#
# # ----------------------------------------------------------------------
# # 8. Snapshot ts 必须是连续竞价时间段
# # ----------------------------------------------------------------------
# def test_snapshot_in_continuous_trading_time(symbol="001287", date="2025-11-07"):
#     df = load_snapshot(symbol, date)
#     for t in df["ts"]:
#         assert t.time() >= datetime.strptime("09:30:00", "%H:%M:%S").time()
#         assert t.time() <= datetime.strptime("15:00:00", "%H:%M:%S").time()
