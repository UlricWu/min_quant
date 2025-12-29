# from __future__ import annotations
#
# import pyarrow as pa
# import pyarrow.parquet as pq
#
# from src.pipeline.context import PipelineContext
# from src.engines.minute_order_agg_engine import MinuteOrderAggEngine
# from src.steps.minute_order_agg_step import MinuteOrderAggStep
#
# from contextlib import contextmanager
#
# class DummyInst:
#     @contextmanager
#     def timer(self, name):
#         yield
# def test_step_skips_missing_input(tmp_path, make_test_pipeline_context):
#     symbol_dir = tmp_path / "symbol" / "000001"
#     symbol_dir.mkdir(parents=True)
#
#     ctx = make_test_pipeline_context(tmp_path)
#
#     step = MinuteOrderAggStep(MinuteOrderAggEngine(),inst=DummyInst())
#     step.run(ctx)
#
#     assert not (symbol_dir / "minute_order.parquet").exists()
#
# #
# # def test_step_runs_successfully(tmp_path, write_parquet, make_test_pipeline_context):
# #     ctx = make_test_pipeline_context(tmp_path)
# #
# #
# #     in_path = ctx.fact_dir / "orderbook_events.parquet"
# #     out_path = ctx.fact_dir / "minute_order.parquet"
# #
# #     rows = [
# #         {
# #             "ts": 0,
# #             "event": "ADD",
# #             "order_id": 1,
# #             "side": "B",
# #             "price": 10.0,
# #             "volume": 10,
# #             "notional": 100.0,
# #         }
# #     ]
# #
# #     schema = pa.schema(
# #         [
# #             ("ts", pa.int64()),
# #             ("event", pa.string()),
# #             ("order_id", pa.int64()),
# #             ("side", pa.string()),
# #             ("price", pa.float64()),
# #             ("volume", pa.int64()),
# #             ("notional", pa.float64()),
# #         ]
# #     )
# #
# #     write_parquet(in_path, rows, schema)
# #
# #
# #
# #     step = MinuteOrderAggStep(MinuteOrderAggEngine(),inst=DummyInst())
# #     step.run(ctx)
# #
# #     assert out_path.exists()
