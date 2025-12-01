#!filepath: src/dataloader/symbol_router.py

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.filesystem import FileSystem
from src.utils.path import PathManager
from src import logs
from src.dataloader.router_metadata import RouterMetadata


class SymbolRouter:
    """
    v1：只按 SecurityID 首字符过滤（0/3/6）
    """
    ALLOWED_PREFIX = ("0", "3", "6")

    def __init__(self, symbols: list = None) -> None:
        self.meta = RouterMetadata()
        self.symbols = [int(s) for s in symbols] if symbols else None

    def route_date(self, date: str):
        logs.info(f"[SymbolRouter] ==== Symbol 路由 date={date} ====")

        date_dir = PathManager.parquet_dir() / date
        if not date_dir.exists():
            logs.warning(f"[route_date] date_dir={date_dir} does not exist")
            return

        logs.info(f"[SymbolRouter] 处理日期 {date}")
        self.meta.reset()
        logs.info(f"[symbol lists]={self.symbols}")

        for parquet_file in date_dir.glob("*.parquet"):
            # 记录 input parquet 信息
            rows = pq.read_table(parquet_file).num_rows
            self.meta.add_input_file(parquet_file.name, rows)

            self._route_single_parquet(parquet_file, date)

        # 最终写入 metadata
        self.meta.save(date)
        logs.info(f"[SymbolRouter] ==== 完成 {date} ====")


    # ----------------------------------------------------
    @logs.catch()
    def _route_single_parquet(self, parquet_path: Path, date: str):
        logs.info(f"[SymbolRouter] 正在拆分 {parquet_path}")

        table = pq.read_table(parquet_path)
        if "SecurityID" not in table.schema.names:
            logs.warning(f"{parquet_path} 缺少 SecurityID")
            return

        security_ids = table.column("SecurityID").to_pylist()
        unique_sids = sorted(set(security_ids))

        for sid in unique_sids:
            if sid is None:
                continue
            symbol = str(sid)
            if len(symbol) < 6:
                symbol = '0' * (6 - len(symbol)) + symbol
            # sid = 507 type=<class 'int'>, symbol = '000507'

            if not symbol.startswith(SymbolRouter.ALLOWED_PREFIX):
                self.meta.add_filtered(symbol)  # 过滤非法 symbol
                continue

            if self.symbols and sid not in self.symbols:  # int
                continue

            logs.info(f"[symbol->parquet] symbol={symbol}")

            mask = [x == sid for x in security_ids]
            sub_table = table.filter(pa.array(mask))

            out_dir = PathManager.data_dir() / "symbol" / symbol /date
            FileSystem.ensure_dir(out_dir)

            out_path = out_dir / f"{parquet_path.name.split('_')[1]}" # SZ_Order.parquet
            logs.info(f"[write_table] out_path={out_path} ")
            pq.write_table(sub_table, out_path, compression="zstd")

            self.meta.add_symbol_output(symbol, sub_table.num_rows)

        logs.info(f"[SymbolRouter] 完成拆分: {parquet_path}")
