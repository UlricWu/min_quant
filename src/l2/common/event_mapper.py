#!filepath: src/l2/common/event_mapper.py

from __future__ import annotations
import pyarrow as pa
import pyarrow.compute as pc
from typing import Dict, Literal, Any

from .exchange_registry import EXCHANGE_REGISTRY
from src.utils.datetime_utils import DateTimeUtils as dt


InternalEventSchema = pa.schema([
    pa.field("ts", pa.timestamp("ns", tz="Asia/Shanghai")),
    pa.field("event", pa.string()),
    pa.field("order_id", pa.int64()),
    pa.field("side", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("buy_no", pa.int64()),
    pa.field("sell_no", pa.int64()),
])


class EventMapper:
    """
    输入：Arrow Batch（L2）
    输出：InternalEvent Batch（统一事件结构）
    """

    def __init__(self, exchange_id: int, kind: Literal["order", "trade"]):
        self.defn = EXCHANGE_REGISTRY[exchange_id][kind]

    # ----------------------------------------------
    def map_batch(self, batch: pa.RecordBatch, trade_date: str) -> pa.RecordBatch:
        """
        逐 batch 映射，不依赖 pandas，不依赖外部字段结构
        """

        # 解析时间
        tick = batch[self.defn.time_field]
        parsed = pc.cast(
            pc.scalar(
                [dt.combine_date_tick(trade_date, dt.parse_tick_time(x))
                 for x in tick.to_pylist()]
            ),
            pa.timestamp("ns", tz="Asia/Shanghai")
        )

        # event 字段
        raw_ev = batch[self.defn.event_field]
        ev_arr = pc.replace_with_mask(
            raw_ev,
            pc.not_equal(raw_ev, raw_ev),
            pa.scalar("UNKNOWN")
        )
        ev_out = pa.array([self.defn.event_mapping.get(v, None) for v in raw_ev.to_pylist()], type=pa.string())

        # side
        if self.defn.side_field:
            raw_side = batch[self.defn.side_field]
            side_out = pa.array([self.defn.side_mapping.get(v, None)
                                 for v in raw_side.to_pylist()], pa.string())
        else:
            side_out = pa.array([None] * batch.num_rows, pa.string())

        return pa.RecordBatch.from_arrays(
            arrays=[
                parsed,
                ev_out,
                pc.cast(batch[self.defn.id_field], pa.int64()),
                side_out,
                pc.cast(batch[self.defn.price_field], pa.float64()),
                pc.cast(batch[self.defn.volume_field], pa.int64()),
                pc.cast(batch[self.defn.buy_no_field], pa.int64()) if self.defn.buy_no_field else pa.array([0] * batch.num_rows, pa.int64()),
                pc.cast(batch[self.defn.sell_no_field], pa.int64()) if self.defn.sell_no_field else pa.array([0] * batch.num_rows, pa.int64()),
            ],
            schema=InternalEventSchema
        )
