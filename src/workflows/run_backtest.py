from src.backtest.core.event_loop import run_event_loop
from src.backtest.data.data_handler import DummyDataHandler
from src.backtest.impl.strategy_dummy import DummyStrategy
from src.backtest.impl.portfolio_dummy import DummyPortfolio
from src.backtest.impl.execution_dummy import DummyExecution


def main() -> None:
    run_event_loop(
        data=DummyDataHandler(),
        strategy=DummyStrategy(),
        portfolio=DummyPortfolio(),
        execution=DummyExecution(),
    )


if __name__ == "__main__":
    main()
