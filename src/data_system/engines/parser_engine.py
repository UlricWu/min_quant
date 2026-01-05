# =============================================================================
# Internal Event Schema（唯一真相）
# =============================================================================
# from __future__ import annotations
from dataclasses import dataclass, asdict

from datetime import datetime
from typing import Dict, Optional
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc

from src import DateTimeUtils

EventKind = Literal["order", "trade"]


@dataclass(slots=True)
class NormalizedEvent:
    symbol: str
    ts: int
    event: str
    order_id: int
    side: str | None
    price: float
    volume: int
    buy_no: int
    sell_no: int

    @classmethod
    def from_row(cls, row):
        return cls(
            symbol=str(row.symbol),
            ts=int(row.ts),
            event=row.fact_dir,
            order_id=int(row.order_id),
            side=None if row.side != row.side else row.side,
            price=float(row.price),
            volume=int(row.volume),
            buy_no=int(row.buy_no),
            sell_no=int(row.sell_no),
        )

    def to_dict(self) -> dict:
        return asdict(self)


INTERNAL_SCHEMA = pa.schema(
    [('symbol', pa.string()),
     ("ts", pa.int64()),
     ("event", pa.string()),
     ("order_id", pa.int64()),
     ("side", pa.string()),
     ("price", pa.float64()),
     ("volume", pa.int64()),
     ("buy_no", pa.int64()),
     ("sell_no", pa.int64()),
     ]
)


@dataclass(frozen=True)
class ExchangeDefinition:
    symbol_field: str
    time_field: str
    event_field: str
    event_mapping: Dict
    price_field: str
    volume_field: str
    side_field: Optional[str]
    side_mapping: Optional[Dict]
    id_field: str
    buy_no_field: Optional[str]
    sell_no_field: Optional[str]


EXCHANGE_REGISTRY = {
    # 上海
    'sh': {
        "order": ExchangeDefinition(
            symbol_field='SecurityID',
            time_field="TickTime",
            event_field="TickType",
            event_mapping={"A": "ADD", "D": "CANCEL"},
            price_field="Price",
            volume_field="Volume",
            side_field="Side",
            side_mapping={'1': "B", '2': "S"},
            id_field="SubSeq",
            buy_no_field=None,
            sell_no_field=None,
        ),
        "trade": ExchangeDefinition(
            symbol_field='SecurityID',
            time_field="TickTime",
            event_field="TickType",
            event_mapping={"T": "TRADE"},
            price_field="Price",
            volume_field="Volume",
            side_field="Side",
            side_mapping={'1': "B", '2': "S"},
            id_field="SubSeq",
            buy_no_field="BuyNo",
            sell_no_field="SellNo",
        ),
    },

    # 深圳
    'sz': {
        "order": ExchangeDefinition(
            symbol_field='SecurityID',
            time_field="OrderTime",
            event_field="OrderType",
            event_mapping={'0': "CANCEL", '1': "ADD", '2': "ADD", '3': "ADD"},
            price_field="Price",
            volume_field="Volume",
            side_field="Side",
            side_mapping={'1': "B", '2': "S"},
            id_field="SubSeq",
            buy_no_field=None,
            sell_no_field=None,
        ),

        "trade": ExchangeDefinition(
            symbol_field='SecurityID',
            time_field="TickTime",
            event_field="ExecType",
            event_mapping={'1': "TRADE", '2': "CANCEL"},
            price_field="TradePrice",
            volume_field="TradeVolume",
            side_field=None,
            side_mapping=None,
            id_field="SubSeq",
            buy_no_field="BuyNo",
            sell_no_field="SellNo",
        ),
    },
}


# MAPPING_kind = {
#     '1':'order',
#     '2':'trade',
# }

# =============================================================================
# 2. TickTime -> offset_us （执行层：Arrow vectorized）
# =============================================================================


def trade_time_to_base_us(trade_time) -> int:
    """
    使用 DateTimeUtils 作为唯一语义来源
    """
    d = DateTimeUtils.extract_date(trade_time)

    base_dt = datetime(
        d.year,
        d.month,
        d.day,
        tzinfo=DateTimeUtils.SH_TZ,
    )
    return int(base_dt.timestamp() * 1_000_000)


def _mod(a: pa.Array, b: int) -> pa.Array:
    """
    Arrow-safe modulo, version independent:
        a % b == a - floor(a / b) * b
    """
    return pc.subtract(
        a,
        pc.multiply(
            pc.cast(pc.floor(pc.divide(a, b)), pa.int64()),
            pa.scalar(b, pa.int64()),
        ),
    )


def tick_to_offset_us(col: pa.Array) -> pa.Array:
    t = pc.cast(col, pa.int64())

    # HH
    hh = pc.cast(pc.floor(pc.divide(t, 1_000_000)), pa.int64())

    # MM
    mm_all = pc.cast(pc.floor(pc.divide(t, 10_000)), pa.int64())
    mm = _mod(mm_all, 100)

    # SS
    ss_all = pc.cast(pc.floor(pc.divide(t, 100)), pa.int64())
    ss = _mod(ss_all, 100)

    # mmm (milliseconds)
    ms = _mod(t, 1_000)

    return pc.add(
        pc.add(
            pc.add(
                pc.multiply(hh, pa.scalar(3_600_000_000, pa.int64())),
                pc.multiply(mm, pa.scalar(60_000_000, pa.int64())),
            ),
            pc.multiply(ss, pa.scalar(1_000_000, pa.int64())),
        ),
        pc.multiply(ms, pa.scalar(1_000, pa.int64())),
    )


def map_dict(col: pa.Array, mapping: dict) -> pa.Array:
    keys = pa.array(list(mapping.keys()))
    vals = pa.array(list(mapping.values()))
    idx = pc.index_in(col, keys)
    return pc.take(vals, idx)


def zeros(n: int) -> pa.Array:
    return pa.array([0] * n, type=pa.int64())


def parse_events_arrow(
        table: pa.Table,
        # kind: Literal["order", "trade"] = '',
        kind: str = '',
        exchange: str = ''
) -> pa.Table:
    """
    输入：
        Arrow Table（单 symbol / 单 kind / 单 exchange）
    输出：
        Arrow Table（InternalEvent schema）
    """
    if table.num_rows == 0:
        return pa.Table.from_arrays([])

    try:
        definition = EXCHANGE_REGISTRY[exchange][kind]
    except KeyError:
        raise KeyError(f"No registry for exchange={exchange}, kind={kind}")
    # --------------------------------------------------
    # 1. base date 来自 TradeTime（包含日期）
    # --------------------------------------------------
    if "TradeTime" not in table.column_names:
        raise ValueError(
            "parse_events_arrow requires TradeTime column for base date"
        )

    base_us = trade_time_to_base_us(
        table["TradeTime"][0].as_py()
    )

    # --------------------------------------------------
    # 2. 日内 offset 来自 definition.time_field
    # --------------------------------------------------
    offset_us = tick_to_offset_us(
        table[definition.time_field]
    )

    ts = pc.add(offset_us, pa.scalar(base_us, pa.int64()))

    # ---------------------------------------------------------------------
    # event
    # ---------------------------------------------------------------------
    event = map_dict(table[definition.event_field], definition.event_mapping)

    # ---------------------------------------------------------------------
    # side
    # ---------------------------------------------------------------------
    if definition.side_field and definition.side_mapping:
        side = map_dict(table[definition.side_field], definition.side_mapping)
    else:
        side = pa.nulls(table.num_rows)
    #
    # ---------------------------------------------------------------------
    # buy / sell no
    # ---------------------------------------------------------------------
    buy_no = (
        table[definition.buy_no_field]
        if definition.buy_no_field
        else zeros(table.num_rows)
    )
    sell_no = (
        table[definition.sell_no_field]
        if definition.sell_no_field
        else zeros(table.num_rows)
    )
    out = pa.table(
        {"symbol": pc.cast(table[definition.symbol_field], pa.string()),
         "ts": ts,
         "event": event,
         "order_id": pc.cast(table[definition.id_field], pa.int64()),
         "side": side,
         "price": pc.cast(table[definition.price_field], pa.float64()),
         "volume": pc.cast(table[definition.volume_field], pa.int64()),
         "buy_no": pc.cast(buy_no, pa.int64()),
         "sell_no": pc.cast(sell_no, pa.int64()),
         }
    )

    return out.cast(INTERNAL_SCHEMA)

# parse_events = parse_events_arrow
