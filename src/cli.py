#!filepath: src/cli.py
import typer
from src.dataloader.pipeline import DataPipeline

app = typer.Typer(help="MinQuant Data Pipeline CLI")

@app.command()
def run(date: str):
    """运行指定日期的数据管线"""
    import pandas as pd
    start_date = '2025-11-03'
    end_date = '2025-11-12'
    dates = pd.date_range(start_date, end_date)
    p = DataPipeline()
    for d in dates:
        d = d.strftime("%Y-%m-%d")
        # if d == '2025-11-23':
        #     continue
        p.run(d)
        # break

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


# @app.command()
# def l2():
#     from src.l2.loader import L2EventLoader
#     from src.l2.feature_engine import L2FeatureEngine
#
#     loader = L2EventLoader(symbol="001287", date="2025-11-03")
#     events = loader.load_events()
#
#     fe = L2FeatureEngine()
#
#     for ev in events:
#         fe.on_event(ev)
#         print(fe.last_snapshot)
        # feats = fe.current_features()
        # TODO: 写入特征表 / 喂给模型 / 生成标签


if __name__ == "__main__":
    app()
# python -m src.cli run 2025-11-23