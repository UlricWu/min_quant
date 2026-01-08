# src/cli.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

import typer

from src import logs
from src.config.app_config import AppConfig
from src.workflows.offline_l2_data import build_offline_l2_pipeline
from src.workflows.offline_l1_backtest import build_offline_l1_backtest
from src.workflows.offline_training import build_offline_training
from src.workflows.experiment_train_backtest import run_train_then_backtest

from src.utils.path import PathManager
from src.meta.symbol_slice_resolver import SymbolSliceResolver
from src.utils.errors import UserInputError

app = typer.Typer(help="MinQuant CLI")


# =============================================================================
# Utilities
# =============================================================================
def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _load_cfg(config_json: Optional[str]) -> AppConfig:
    """
    Load AppConfig with optional JSON override.
    """
    override = json.loads(config_json) if config_json else None
    return AppConfig.load(override=override)


# =============================================================================
# Preflight (Backtest)
# =============================================================================
def _validate_date_has_feature_meta(
    pm: PathManager,
    date: str,
) -> tuple[bool, str]:
    """
    Check whether feature meta exists for this date.
    """
    meta_dir = pm.meta_dir(date)
    try:
        # feature stage 是 backtest 的硬依赖
        SymbolSliceResolver(meta_dir=meta_dir, stage="feature")
        return True, ""
    except Exception as e:
        return (
            False,
            f"no feature meta for date={date} meta_dir={meta_dir} err={e!r}",
        )


def _preflight_backtest_inputs(cfg_backtest) -> None:
    """
    Fail fast on invalid backtest inputs.
    This is a USER INPUT validation, not a system error.
    """
    pm = PathManager()

    bad_dates: list[str] = []

    for d in cfg_backtest.dates:
        ok, _ = _validate_date_has_feature_meta(pm, d)
        if not ok:
            bad_dates.append(d)

    if bad_dates:
        raise UserInputError(
            "[BacktestInputError] invalid dates: "
            f"{bad_dates}. "
            "These dates do not have feature meta "
            "(run L2 pipeline first or choose valid dates)."
        )


# =============================================================================
# Commands
# =============================================================================
@app.command()
def run(date: str):
    """
    Run L2 pipeline for a single date.
    """
    logs.init(scope="job")
    logs.info(f"[CLI] run L2 pipeline date={date}")

    build_offline_l2_pipeline().run(date)


@app.command()
def backtest(
    run_id: Optional[str] = None,
    config_json: Optional[str] = None,
):
    """
    Run backtest with optional injected config.
    """
    if run_id is None:
        run_id = _today()

    logs.init(scope="backtest", run_id=run_id)
    logs.info(f"[CLI] backtest start run_id={run_id}")

    try:
        cfg = _load_cfg(config_json)

        # 🔒 preflight: user input validation
        _preflight_backtest_inputs(cfg.backtest)

        pipeline = build_offline_l1_backtest(cfg=cfg.backtest)
        pipeline.run(run_id)

        logs.info(f"[CLI] backtest done run_id={run_id}")

    except UserInputError as e:
        # 用户输入错误：不打印 traceback
        logs.error(str(e))
        raise SystemExit(1)


@app.command()
def train(
    run_id: Optional[str] = None,
    config_json: Optional[str] = None,
):
    """
    Run offline training with optional injected config.
    """
    if run_id is None:
        run_id = _today()

    logs.init(scope="train", run_id=run_id)
    logs.info(f"[CLI] train start run_id={run_id}")

    try:
        cfg = _load_cfg(config_json)

        # NOTE:
        # 目前 training 默认信任输入
        # 如果未来需要，可在此加入：
        #   _preflight_training_inputs(cfg.training)

        pipeline = build_offline_training(cfg=cfg.training)
        pipeline.run(run_id)

        logs.info(f"[CLI] train done run_id={run_id}")

    except UserInputError as e:
        logs.error(str(e))
        raise SystemExit(1)


@app.command()
def experiment(
    run_id: Optional[str] = None,
    config_json: Optional[str] = None,
):
    """
    Run experiment: train -> promote -> backtest.
    """
    if run_id is None:
        run_id = f"exp_{_today()}"

    logs.init(scope="experiment", run_id=run_id)
    logs.info(f"[CLI] experiment start run_id={run_id}")

    try:
        cfg = _load_cfg(config_json)

        pipeline = run_train_then_backtest(cfg=cfg)
        pipeline.run(run_id=run_id)

        logs.info(f"[CLI] experiment done run_id={run_id}")

    except UserInputError as e:
        logs.error(str(e))
        raise SystemExit(1)


# =============================================================================
# Entry
# =============================================================================
if __name__ == "__main__":
    app()
