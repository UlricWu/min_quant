#!filepath: src/cli.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import typer
from rich import print

from src import PathManager, logs
from src.workflows.offline_l2_data import build_offline_l2_pipeline
from src.workflows.offline_l1_backtest import build_offline_l1_backtest
from src.workflows.offline_training import build_offline_training
from src.workflows.experiment_train_backtest import run_train_then_backtest
from src.utils.SourceMetaRepairTool import SourceMetaRepairTool

app = typer.Typer(help="MinQuant Data Pipeline CLI")


# ============================================================================
# Utilities
# ============================================================================
def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


# ============================================================================
# Commands
# ============================================================================
@app.command()
def version():
    """
    Show CLI version
    """
    print("v0.1.0")


@app.command()
def run(date: str):
    """
    Run L2 pipeline for a specific date (YYYY-MM-DD)
    """
    # L2 pipeline 不属于 train/backtest run
    # 日志统一记为 job 级（由 JobRunner 注入 job_id）
    logs.init(scope="job")

    logs.info(f"[CLI] run L2 pipeline date={date}")
    print(f"[green]Running L2 Pipeline for {date}[/green]")

    pipeline = build_offline_l2_pipeline()
    pipeline.run(date)


@app.command()
def range(start: str, end: str):
    """
    Run L2 pipeline for a date range (YYYY-MM-DD)
    """
    logs.init(scope="job")

    logs.info(f"[CLI] run L2 pipeline range {start} -> {end}")
    print(f"[blue]Running L2 Pipeline for range {start} -> {end}[/blue]")

    pipeline = build_offline_l2_pipeline()
    dates = pd.date_range(start, end)

    for d in dates:
        pipeline.run(d.strftime("%Y-%m-%d"))


@app.command()
def today():
    """
    Run L2 pipeline for today
    """
    logs.init(scope="job")

    date = _today()
    logs.info(f"[CLI] run L2 pipeline today={date}")
    print(f"[yellow]Running L2 Pipeline for today: {date}[/yellow]")

    pipeline = build_offline_l2_pipeline()
    pipeline.run(date)


@app.command()
def backtest(run_id: str | None = None):
    """
    Run Level-1 backtest
    """
    if run_id is None:
        run_id = _today()

    # backtest 是 run 级别语义
    logs.init(scope="backtest", run_id=run_id)

    logs.info(f"[CLI] backtest start run_id={run_id}")
    print(f"[magenta]Running L1 Backtest | run_id={run_id}[/magenta]")

    pipeline = build_offline_l1_backtest()
    pipeline.run(run_id)

    logs.info(f"[CLI] backtest done run_id={run_id}")


@app.command()
def train(run_id: str | None = None):
    """
    Run offline training
    """
    if run_id is None:
        run_id = _today()

    # training 是 run 级别语义
    logs.init(scope="train", run_id=run_id)

    logs.info(f"[CLI] training start run_id={run_id}")
    print(f"[magenta]Running offline_training | run_id={run_id}[/magenta]")

    pipeline = build_offline_training()
    pipeline.run(run_id)

    logs.info(f"[CLI] training done run_id={run_id}")


@app.command()
def experiment(run_id: str | None = None):
    """
    Run experiment: train -> promote -> backtest
    """
    if run_id is None:
        run_id = f"exp_{_today()}"

    # experiment 是独立 run 语义
    logs.init(scope="experiment", run_id=run_id)

    logs.info(f"[CLI] experiment start run_id={run_id}")
    print(f"[cyan]Running experiment | run_id={run_id}[/cyan]")

    pipeline = run_train_then_backtest()
    pipeline.run(run_id=run_id)

    logs.info(f"[CLI] experiment done run_id={run_id}")


@app.command()
def repair(start_date: str, end_date: str):
    """
    Repair source (download) meta for existing raw files
    """
    # repair 是运维工具，归入 system/job 级
    logs.init(scope="system")

    logs.info(f"[CLI] repair meta range {start_date} -> {end_date}")
    print(f"[green]Repairing source meta {start_date} -> {end_date}[/green]")

    tool = SourceMetaRepairTool(pm=PathManager())
    tool.repair_range(start_date, end_date)

    logs.info("[CLI] repair done")


# ============================================================================
# Entry
# ============================================================================
if __name__ == "__main__":
    app()
