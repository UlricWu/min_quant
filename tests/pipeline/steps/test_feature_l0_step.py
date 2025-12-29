# from __future__ import annotations
#
# import time
# from pathlib import Path
#
# import pyarrow as pa
# import pyarrow.parquet as pq
# import pandas as pd
# import pytest
#
# from src.engines.feature_l0_engine import FeatureL0Engine
# # from src.steps.feature_l0_step import FeatureL0Step
#
#
# # -----------------------------------------------------------------------------
# # helpers
# # -----------------------------------------------------------------------------
# def write_minimal_fact(path: Path, *, rows: int = 2) -> None:
#     df = pd.DataFrame(
#         {
#             "open": list(range(rows)),
#             "high": list(range(rows)),
#             "low": list(range(rows)),
#             "close": list(range(rows)),
#             "volume": [10] * rows,
#             "trade_count": [5] * rows,
#         }
#     )
#     pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)
#
#
#
#
# # -----------------------------------------------------------------------------
# # 1. 首次运行：应生成 feature 并写 meta
# # -----------------------------------------------------------------------------
# def test_feature_l0_step_first_run_writes_output_and_meta(
#     make_test_pipeline_context,
# ):
#     ctx = make_test_pipeline_context()
#
#     fact_file = ctx.fact_dir / "sh_trade_min.parquet"
#     write_minimal_fact(fact_file)
#
#     engine = FeatureL0Engine()
#     step = FeatureL0Step(engine=engine)
#
#     step.run(ctx)
#
#     out_file = ctx.feature_dir / "sh_trade_l0.parquet"
#     assert out_file.exists(), "FeatureL0 output not written"
#
#     # meta 目录应非空
#     assert any(ctx.meta_dir.iterdir()), "Meta not written on first run"
#
#
# # -----------------------------------------------------------------------------
# # 2. 第二次运行（无 upstream 变化）：必须 skip
# # -----------------------------------------------------------------------------
# def test_feature_l0_step_skips_when_upstream_unchanged(
#     make_test_pipeline_context,
# ):
#     ctx = make_test_pipeline_context()
#
#     fact_file = ctx.fact_dir / "sh_trade_min.parquet"
#     write_minimal_fact(fact_file)
#
#     engine = FeatureL0Engine()
#     step = FeatureL0Step(engine=engine)
#
#     # 第一次 run
#     step.run(ctx)
#
#     out_file = ctx.feature_dir / "sh_trade_l0.parquet"
#     assert out_file.exists()
#
#     mtime_first = out_file.stat().st_mtime
#
#     time.sleep(0.1)
#
#     # 第二次 run（fact 未变）
#     step.run(ctx)
#
#     mtime_second = out_file.stat().st_mtime
#
#     # 输出文件不应被重写
#     assert (
#         mtime_second == mtime_first
#     ), "Output was rewritten despite no upstream change"
#
#
# # -----------------------------------------------------------------------------
# # 3. upstream 变化后：必须重算
# # -----------------------------------------------------------------------------
# def test_feature_l0_step_reruns_when_upstream_changes(
#     make_test_pipeline_context,
# ):
#     ctx = make_test_pipeline_context()
#
#     fact_file = ctx.fact_dir / "sh_trade_min.parquet"
#     write_minimal_fact(fact_file)
#
#     engine = FeatureL0Engine()
#     step = FeatureL0Step(engine=engine)
#
#     # 第一次 run
#     step.run(ctx)
#
#     out_file = ctx.feature_dir / "sh_trade_l0.parquet"
#     mtime_first = out_file.stat().st_mtime
#
#     time.sleep(0.1)
#
#     # 内容真的变了
#     write_minimal_fact(fact_file, rows=27)
#     step.run(ctx)
#
#     # 第二次 run（upstream 变化）
#     step.run(ctx)
#
#     mtime_second = out_file.stat().st_mtime
#
#     assert (
#         mtime_second > mtime_first
#     ), "Output not updated after upstream change"
