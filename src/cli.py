#!filepath: src/cli.py
import pandas as pd
import typer
from rich import print

from src.workflows.offline_l2_pipeline import build_offline_l2_pipeline

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
def backtest(date: str, symbol: str):
    """
    Run Level-1 backtest
    """
    from src.workflows.backtest_l1_workflow import run_backtest_l1

    equity = run_backtest_l1(date=date, symbol=symbol)
    print(equity.tail())



if __name__ == "__main__":
    app()

# python -m src.cli run 2025-11-04
