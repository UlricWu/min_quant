# # tests/normalize/test_normalize_trade_sh_sz.py
# import pandas as pd
# from src.engines.normalize_engine import NormalizeEngine
# from src.engines.context import EngineContext
#
#
# def test_trade_normalize_sh_sz(
#         parquet_input_dir,
#         canonical_output_dir,
#         date,
# ):
#     engine = NormalizeEngine()
#
#     ctx = EngineContext(
#         mode="offline",
#         date=date,
#         input_path=parquet_input_dir,
#         output_path=canonical_output_dir,
#     )
#     engine.execute(ctx)
#
#     sh = pd.read_parquet(canonical_output_dir / "SH_Trade.parquet")
#     sz = pd.read_parquet(canonical_output_dir / "SZ_Trade.parquet")
#
#     # SH：2 条合法（非法 symbol / ExecType 被过滤）
#     assert len(sh) == 2
#     assert set(sh["symbol"]) == {"600000"}
#
#     # SZ：1 条
#     assert len(sz) == 1
#     assert set(sz["symbol"]) == {"002936"}
#
#
# # tests/normalize/test_outputs_exist.py
# from src.engines.normalize_engine import NormalizeEngine
# from src.engines.context import EngineContext
