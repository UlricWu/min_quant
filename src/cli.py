#!filepath: src/cli.py
import typer
from src.data.pipeline import DataPipeline

app = typer.Typer(help="MinQuant Data Pipeline CLI")

@app.command()
def run(date: str):
    """运行指定日期的数据管线"""
    p = DataPipeline()
    p.run(date)

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