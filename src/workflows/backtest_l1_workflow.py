# src/workflows/backtest_l1_workflow.py
from __future__ import annotations

from typing import Iterable, List

import pandas as pd

from src.utils.path import PathManager
from src.observability.instrumentation import Instrumentation

# backtest core
from src.backtest.events import MarketEvent
from src.backtest.backtest import BacktestEngine
from src.backtest.alpha import AlphaStrategy
from src.backtest.portfolio import Portfolio
from src.backtest.execution import ExecutionEngine


# =============================================================================
# Public API
# =============================================================================

def run_backtest_l1(
    *,
    date: str,
    symbol: str,
    initial_cash: float = 1_000_000.0,
) -> pd.DataFrame:
    """
    Run Level-1 (Bar-driven) backtest for ONE symbol on ONE date.

    Contract:
      - Consume ONLY offline assets produced by Offline L2 Pipeline
      - No dependency on pipeline ctx / steps
      - Stateless, reproducible

    Inputs:
      - minute bar parquet
      - feature parquet

    Output:
      - equity curve DataFrame
    """

    inst = Instrumentation()
    pm = PathManager()

    inst.info(f"[BacktestL1] start date={date} symbol={symbol}")

    # ---------------------------------------------------------------------
    # 1. Load offline assets
    # ---------------------------------------------------------------------
    bars = _load_minute_bars(pm, date, symbol)
    feats = _load_features(pm, date, symbol)

    df = _align_bar_and_feature(bars, feats)

    if df.empty:
        inst.warn(f"[BacktestL1] empty data: {date} {symbol}")
        return pd.DataFrame()

    # ---------------------------------------------------------------------
    # 2. Build MarketEvent stream (Level-1)
    # ---------------------------------------------------------------------
    market_events = _build_market_events(df)

    # ---------------------------------------------------------------------
    # 3. Initialize backtest components
    # ---------------------------------------------------------------------
    alpha = AlphaStrategy(feature_df=df)
    portfolio = Portfolio(cash=initial_cash)
    execution = ExecutionEngine(commission_per_trade=1.0)

    engine = BacktestEngine(
        market_events=market_events,
        alpha=alpha,
        portfolio=portfolio,
        execution=execution,
    )

    # ---------------------------------------------------------------------
    # 4. Run backtest
    # ---------------------------------------------------------------------
    engine.run()

    equity = pd.DataFrame(
        {
            "ts": df["ts"].values,
            "equity": engine.equity_curve,
        }
    )

    inst.info(
        f"[BacktestL1] done date={date} symbol={symbol} "
        f"final_equity={equity['equity'].iloc[-1]:.2f}"
    )

    return equity


# =============================================================================
# Internal helpers
# =============================================================================

def _load_minute_bars(
    pm: PathManager,
    date: str,
    symbol: str,
) -> pd.DataFrame:
    """
    Load minute bar produced by MinuteTradeAggStep.
    """
    path = pm.minute_trade_bar(date, symbol)

    if not path.exists():
        raise FileNotFoundError(f"Minute bar not found: {path}")

    df = pd.read_parquet(path)

    _require_columns(
        df,
        [
            "ts",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
        name="minute_bar",
    )

    return df.sort_values("ts")


def _load_features(
    pm: PathManager,
    date: str,
    symbol: str,
) -> pd.DataFrame:
    """
    Load feature table produced by FeatureBuildStep.
    """
    path = pm.feature(date, symbol)

    if not path.exists():
        raise FileNotFoundError(f"Feature file not found: {path}")

    df = pd.read_parquet(path)

    _require_columns(
        df,
        [
            "ts",
            "symbol",
        ],
        name="feature",
    )

    return df.sort_values("ts")


def _align_bar_and_feature(
    bars: pd.DataFrame,
    feats: pd.DataFrame,
) -> pd.DataFrame:
    """
    Left-join feature onto bar by (ts, symbol).

    Design law (FROZEN):
      - Bar is time backbone
      - Feature may be missing (NaN allowed)
    """
    df = bars.merge(
        feats,
        on=["ts", "symbol"],
        how="left",
        suffixes=("", "_feat"),
    )

    return df.sort_values("ts").reset_index(drop=True)


def _build_market_events(
    df: pd.DataFrame,
) -> List[MarketEvent]:
    """
    Convert bar rows into MarketEvent stream.

    Level-1:
      - One MarketEvent per minute bar
      - Feature NOT embedded in event
    """
    events: List[MarketEvent] = []

    for row in df.itertuples(index=False):
        events.append(
            MarketEvent(
                ts=row.ts,
                symbol=row.symbol,
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
            )
        )

    return events


def _require_columns(
    df: pd.DataFrame,
    cols: Iterable[str],
    *,
    name: str,
) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"{name} missing required columns: {missing}"
        )
