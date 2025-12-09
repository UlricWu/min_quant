#!filepath: src/cli.py
import typer
from src.dataloader.pipeline import DataPipeline

app = typer.Typer(help="MinQuant Data Pipeline CLI")


@app.command()
def run(date: str):
    """运行指定日期的数据管线"""
    import pandas as pd
    start_date = '2025-11-03'
    end_date = '2025-11-04'
    dates = pd.date_range(start_date, end_date)
    p = DataPipeline()
    for d in dates:
        d = d.strftime("%Y-%m-%d")
        p.run(d)

#  ftp->7z->csv->parquet->sh_order_trade split->symbol_date ->trade_enrich -> orderbook

@app.command()
def today():
    """运行当天的数据管线"""
    from datetime import datetime
    date = datetime.now().strftime("%Y-%m-%d")
    p = DataPipeline()
    p.run(date)

# TODO: 写入特征表 / 喂给模型 / 生成标签

if __name__ == "__main__":
    app()

# python -m src.cli run 2025-11-23
