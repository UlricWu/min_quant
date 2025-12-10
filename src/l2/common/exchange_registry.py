#!filepath: src/l2/common/exchange_registry.py

from .exchange_def import ExchangeDefinition


EXCHANGE_REGISTRY = {
    # 上海
    1: {
        "order": ExchangeDefinition(
            time_field="TickTime",
            event_field="TickType",
            event_mapping={"A": "ADD", "D": "CANCEL"},
            price_field="Price",
            volume_field="Volume",
            side_field="Side",
            side_mapping={1: "B", 2: "S"},
            id_field="SubSeq",
            buy_no_field=None,
            sell_no_field=None,
        ),
        "trade": ExchangeDefinition(
            time_field="TickTime",
            event_field="TickType",
            event_mapping={"T": "TRADE"},
            price_field="Price",
            volume_field="Volume",
            side_field="Side",
            side_mapping={1: "B", 2: "S"},
            id_field="SubSeq",
            buy_no_field="BuyNo",
            sell_no_field="SellNo",
        ),
    },

    # 深圳
    2: {
        "order": ExchangeDefinition(
            time_field="OrderTime",
            event_field="OrderType",
            event_mapping={0: "CANCEL", 1: "ADD", 2: "ADD", 3: "ADD"},
            price_field="Price",
            volume_field="Volume",
            side_field="Side",
            side_mapping={1: "B", 2: "S"},
            id_field="SubSeq",
            buy_no_field=None,
            sell_no_field=None,
        ),

        "trade": ExchangeDefinition(
            time_field="TickTime",
            event_field="ExecType",
            event_mapping={1: "TRADE", 2: "CANCEL"},
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
