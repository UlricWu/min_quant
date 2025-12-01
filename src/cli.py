#!filepath: src/cli.py
import typer
from src.dataloader.pipeline import DataPipeline

app = typer.Typer(help="MinQuant Data Pipeline CLI")

@app.command()
def run(date: str):
    """运行指定日期的数据管线"""
    import pandas as pd
    start_date = '2025-11-03'
    end_date = '2025-11-28'
    dates = pd.date_range(start_date, end_date)
    p = DataPipeline()
    for d in dates:
        print(d)
        d = d.strftime("%Y-%m-%d")
        if d == '2025-11-23':
            continue
        p.run(d)
        break

    # d = '2025-11-23'
    # p = DataPipeline()
    # p.run(d)

@app.command()
def today():
    """运行当天的数据管线"""
    from datetime import datetime
    date = datetime.now().strftime("%Y-%m-%d")
    p = DataPipeline()
    p.run(date)

if __name__ == "__main__":
    app()
# python -m src.cli run 2025-11-23