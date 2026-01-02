#!filepath: src/cli.py
import pandas as pd
import typer
from rich import print

from src.workflows.offline_l2_data import build_offline_l2_pipeline
from src.workflows.offline_l1_backtest import build_offline_l1_backtest

app = typer.Typer(help="MinQuant Data Pipeline CLI")


@app.command()
def version():
    print("v0.1.0")


@app.command()
def run(date: str):
    """
    运行指定日期的 L2 Pipeline（完整 Step-based Workflow）
    """
    print(f"[green]Running L2 Pipeline for {date}[/green]")

    pipeline = build_offline_l2_pipeline()
    pipeline.run(date)


@app.command()
def range(start: str, end: str):
    """
    连续运行多个日期（YYYY-MM-DD）
    """
    import pandas as pd

    pipeline = build_offline_l2_pipeline()
    dates = pd.date_range(start, end)

    print(f"[blue]Running L2 Pipeline for range {start} -> {end}[/blue]")

    for d in dates:
        d = d.strftime("%Y-%m-%d")
        pipeline.run(d)


@app.command()
def today():
    """
    运行当天的数据管线
    """
    from datetime import datetime

    date = datetime.now().strftime("%Y-%m-%d")
    pipeline = build_offline_l2_pipeline()

    print(f"[yellow]Running L2 Pipeline for today: {date}[/yellow]")
    pipeline.run(date)

@app.command()
def backtest(run_id: str | None = None):
    """
    Run Level-1 backtest (dates defined in YAML)
    """
    if run_id is None:
        from datetime import datetime
        run_id = datetime.now().strftime("%Y-%m-%d")

    pipeline = build_offline_l1_backtest()
    print(f"[magenta]Running L1 Backtest | run_id={run_id}[/magenta]")

    pipeline.run(run_id)


if __name__ == "__main__":
    app()

# python -m src.cli run 2025-11-04
# python -m src.cli backtest
