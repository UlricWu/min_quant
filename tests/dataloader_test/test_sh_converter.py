import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pathlib import Path

from src.dataloader.sh_converter import ShConverter


# ----------------------------------------------------------------------
# 工具函数：生成临时混合 SH parquet 文件
# ----------------------------------------------------------------------
def make_parquet(tmp_path: Path, ticktype_array, name="SH_Mixed.parquet"):
    """
    生成一个包含 TickType + SecurityID + Price 字段的 L2 混合文件
    """
    table = pa.table({
        "TickType": ticktype_array,
        "SecurityID": pa.array([600000, 600000, 600000, 600000]),
        "Price": pa.array([10.1, 10.2, 10.3, 10.4]),
    })
    out = tmp_path / name
    pq.write_table(table, out)
    return out


# ----------------------------------------------------------------------
# 基础测试：string 类型 TickType
# ----------------------------------------------------------------------
def test_sh_converter_string(tmp_path):
    tick = pa.array(["A", "D", "T", "M"], type=pa.string())
    parquet_path = make_parquet(tmp_path, tick)

    conv = ShConverter()
    conv.split(parquet_path)

    out_order = parquet_path.with_name("SH_Order.parquet")
    out_trade = parquet_path.with_name("SH_Trade.parquet")

    assert out_order.exists()
    assert out_trade.exists()

    order_tbl = pq.read_table(out_order)
    trade_tbl = pq.read_table(out_trade)

    # 订单 = A, D, M → 共 3 条
    assert order_tbl.num_rows == 3
    # 成交 = T → 共 1 条
    assert trade_tbl.num_rows == 1


# ----------------------------------------------------------------------
# 二进制类型 TickType
# ----------------------------------------------------------------------
def test_sh_converter_binary(tmp_path):
    tick = pa.array([b"A", b"D", b"T", b"M"], type=pa.binary())
    parquet_path = make_parquet(tmp_path, tick)

    conv = ShConverter()
    conv.split(parquet_path)

    out_order = parquet_path.with_name("SH_Order.parquet")
    out_trade = parquet_path.with_name("SH_Trade.parquet")

    assert out_order.exists()
    assert out_trade.exists()

    order_tbl = pq.read_table(out_order)
    trade_tbl = pq.read_table(out_trade)

    assert order_tbl.num_rows == 3
    assert trade_tbl.num_rows == 1


# ----------------------------------------------------------------------
# Dictionary<string> 类型（你真实 L2 数据最常见）
# ----------------------------------------------------------------------
def test_sh_converter_dictionary(tmp_path):
    dict_type = pa.dictionary(index_type=pa.int32(), value_type=pa.string())
    tick = pa.array(["A", "D", "T", "M"], type=dict_type)

    parquet_path = make_parquet(tmp_path, tick)

    conv = ShConverter()
    conv.split(parquet_path)

    out_order = parquet_path.with_name("SH_Order.parquet")
    out_trade = parquet_path.with_name("SH_Trade.parquet")

    assert out_order.exists()
    assert out_trade.exists()

    order_tbl = pq.read_table(out_order)
    trade_tbl = pq.read_table(out_trade)

    assert order_tbl.num_rows == 3
    assert trade_tbl.num_rows == 1


# ----------------------------------------------------------------------
# 当文件缺 TickType 字段，应跳过且不生成结果
# ----------------------------------------------------------------------
def test_missing_ticktype(tmp_path):
    table = pa.table({
        "SecurityID": pa.array([600000, 600000]),
        "Price": pa.array([10.1, 10.2]),
    })
    parquet_path = tmp_path / "bad.parquet"
    pq.write_table(table, parquet_path)

    conv = ShConverter()
    conv.split(parquet_path)

    # 不应生成任何输出
    assert not parquet_path.with_name("SH_Order.parquet").exists()
    assert not parquet_path.with_name("SH_Trade.parquet").exists()


# ----------------------------------------------------------------------
# 空表测试（即使没有任何 A/D/M/T，也应安全退出）
# ----------------------------------------------------------------------
def test_empty_table(tmp_path):
    # 所有列长度必须一致（这里全部为 0）
    table = pa.table({
        "TickType": pa.array([], type=pa.string()),
        "SecurityID": pa.array([], type=pa.int32()),
        "Price": pa.array([], type=pa.float32()),
    })

    parquet_path = tmp_path / "empty.parquet"
    pq.write_table(table, parquet_path)

    conv = ShConverter()
    conv.split(parquet_path)

    out_order = parquet_path.with_name("SH_Order.parquet")
    out_trade = parquet_path.with_name("SH_Trade.parquet")

    assert out_order.exists()
    assert out_trade.exists()

    assert pq.read_table(out_order).num_rows == 0
    assert pq.read_table(out_trade).num_rows == 0
