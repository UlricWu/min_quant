# src/backtest/steps/replay_price_l1_step.py
from src.pipeline.step import PipelineStep
from src.backtest.engine import BacktestEngine
from src.backtest.replay.multi_symbol import MultiSymbolReplay
from src.backtest.strategy.threshold import ThresholdStrategy
from src.backtest.portfolio.price_driven import PriceDrivenPortfolio


class ReplayPriceL1Step(PipelineStep):
    """
    Price-Driven L1 Replay（FINAL）
    """

    stage = "backtest_replay_price"
    output_slot = "portfolio"

    def __init__(self, *, backtest_cfg, inst=None):
        super().__init__(inst=inst)
        self._bt = backtest_cfg

    def run(self, ctx):
        strategy_cfg = self._bt.strategy

        strategy = ThresholdStrategy(
            feature=strategy_cfg["signal_feature"],
            threshold=strategy_cfg["threshold"],
        )

        portfolio = PriceDrivenPortfolio()

        engine = BacktestEngine(
            strategy=strategy,
            portfolio=portfolio,
            execution=None,
        )

        streams = ctx.data_handler.iter_symbol_events()
        replay = MultiSymbolReplay(streams)

        engine.run(replay.replay())

        ctx.portfolio = portfolio
        return ctx
