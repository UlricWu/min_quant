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
    仅负责 symbol 拆分，不负责 pipeline 调度、不负责计时、不负责进度。
    """
    ALLOWED_PREFIX = ("0", "3", "6")

    def __init__(self, symbols: list = None) -> None:
        self.meta = RouterMetadata()
        self.symbols = [int(s) for s in symbols] if symbols else None

    def route_date(self, date: str):
        logs.info(f"[SymbolRouter] ==== Symbol router date={date} ====")

        date_dir = PathManager.parquet_dir() / date
        if not date_dir.exists():
            logs.warning(f"[route_date] date_dir={date_dir} does not exist")
            return

        self.meta.reset()

        for parquet_file in date_dir.glob("*.parquet"):
            self._route_single_parquet(parquet_file, date)

        # 最终写入 metadata
        self.meta.save(date)
        logs.info(f"[SymbolRouter] ==== 完成 {date} ====")

    # ========================== 核心修改开始 ================================
    def _symbol_matches_exchange(self, symbol: str, is_sh: bool, is_sz: bool):
        """上交所 symbol 以 6 开头；深交所以 0/3 开头"""
        if is_sh:
            return symbol.startswith("6")
        if is_sz:
            return symbol.startswith(("0", "3"))
        return False

    # ----------------------------------------------------
    @logs.catch()
    def _route_single_parquet(self, parquet_path: Path, date: str):
        logs.info(f"[SymbolRouter] 处理 {parquet_path.name}")

        is_sh = parquet_path.name.lower().startswith("sh_")
        is_sz = parquet_path.name.lower().startswith("sz_")
        # ------------------------------
        # 第 1 阶段：快速判断是否需要处理该 parquet
        # ------------------------------
        candidate_symbols = []
        for sid in self.symbols:

            symbol = f"{sid:06d}"
            # 交易所不匹配 → 跳过
            if not self._symbol_matches_exchange(symbol, is_sh, is_sz):
                continue

            # 目标输出文件，例如 Order.parquet / Trade.parquet
            out_file = PathManager.symbol_dir(symbol, date) / parquet_path.name.split("_")[1]

            if out_file.exists():
                logs.debug(f"[smart-skip] {symbol} 目标文件已存在，跳过该 symbol")
                continue  # 不加入候选列表

            # symbol 必须满足过滤前缀
            if not symbol.startswith(self.ALLOWED_PREFIX):
                self.meta.add_filtered(symbol)
                continue

            candidate_symbols.append(symbol)

        # ------------------------------------------------
        # 2）精确跳过 parquet：如果没有 symbol 要处理 → 完全跳过，不读取 parquet
        # ------------------------------------------------
        if not candidate_symbols:
            logs.info(f"[smart-skip] {parquet_path.name} 全部 symbol 已存在 → 跳过该 parquet")
            return

        # ------------------------------------------------
        # 3）真正读取 parquet（只有当需要处理 symbol 时才会进入）
        # ------------------------------------------------
        table = pq.read_table(parquet_path)
        if "SecurityID" not in table.schema.names:
            logs.warning(f"{parquet_path} 缺少 SecurityID")
            return

        security_ids = table.column("SecurityID").to_pylist()
        # ------------------------------------------------
        # 4）逐 symbol 拆分
        # ------------------------------------------------
        for symbol in candidate_symbols:
            mask = [x == int(symbol) for x in security_ids]
            sub_table = table.filter(pa.array(mask))

            if not sub_table.num_rows:
                logs.debug(f"[filter] no data for symbol={symbol}")
                continue

            logs.info(f"[symbol] 正在拆分 symbol={symbol}")

            symbol_path = PathManager.symbol_dir(symbol, date)
            FileSystem.ensure_dir(symbol_path)
            symbol_file = symbol_path / parquet_path.name.split("_")[1]

            logs.info(f"[write] {symbol} → {symbol_file}")
            pq.write_table(sub_table, symbol_file, compression="zstd")

            self.meta.add_symbol_output(symbol, sub_table.num_rows)

        logs.info(f"[SymbolRouter] 完成拆分: {parquet_path}")