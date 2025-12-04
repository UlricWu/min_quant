# min_quant


# 📈 Quant Research & Trading System

A modular, production-ready quantitative research and trading system designed for:

- High-frequency and intraday alpha research  
- Event-driven signals (tick / L2 / orderbook)  
- ML-driven prediction and strategy modeling  
- Walk-Forward training & backtesting  
- Feature/label experimentation  
- Portfolio management and execution simulation  
- Clean, testable, and extensible architecture

This repository provides an end-to-end framework from **data → feature → model → backtest → live**.

---

## 🚀 Project Structure
```
project_root/
│
├── data/ # Local data (ignored by git)
│ ├── raw/ # Tick / Level1 / Level2 / macro
│ ├── processed/ # Cleaned features & labels
│ └── external/ # External datasets (news, macro)
│
├── config/ # All experiment configurations
│ ├── data_config.yaml
│ ├── model_config.yaml
│ ├── strategy_config.yaml
│ └── backtest_config.yaml
│
├── src/
│ ├── data/ # Loader, cleaner, aggregator, feature engineering
│ ├── labels/ # Triple barrier, volatility, meta-labeling
│ ├── models/ # Trainer, predictor, metrics, registry
│ ├── strategy/ # Base strategies + ML-driven strategies
│ ├── risk/ # Risk manager & position sizing
│ ├── execution/ # Execution & slippage simulation
│ ├── portfolio/ # Holdings & performance analytics
│ ├── backtest/ # Backtester + Walk-Forward engine
│ ├── pipeline/ # Build dataset / Train / Backtest workflows
│ ├── api/ # Real-time data & live trading modules
│ └── utils/ # Logger, config loader, path manager
│
├── scripts/ # Helper scripts for CLI / Makefile
├── notebooks/ # Research notebooks
├── tests/ # Unit tests (pytest)
├── logs/ # Runtime logs (ignored)
├── models/ # Trained models (ignored)
│
├── .gitignore
├── Makefile
├── requirements.txt
└── README.md
```

---

## 🧩 Key Features

### **1. Modular, Extensible Architecture**
Everything is separated into modules:
- data pipelines  
- labeling methods  
- ML models  
- trading strategies  
- execution simulation  
- risk management  
- walk-forward backtesting  
- live trading API  

You can replace any module without breaking others.

---

### **2. Feature Engineering for Trading**
Supports:

- Microprice, OFI, VPIN  
- Price impact / orderbook pressure  
- Volume imbalance  
- VWAP / TICK compressions  
- Event-driven features (orderbook changes, cluster events)

---

### **3. Labeling Framework**

- Triple-Barrier method  
- Volatility estimation  
- Meta-labeling pipeline  
- Side/size predictions (direction + confidence)

---

### **4. ML Modeling System**

- Unified trainer interface  
- LightGBM / XGBoost / CatBoost / SKLearn models  
- Feature set registry  
- Model versioning  
- Predictor for offline & online inference  
- Metric suite (AUC, precision, SR, DD, hit ratio)

---

### **5. Backtesting Engine**

Supports:

- Event-driven architecture  
- Multiple symbols  
- Slippage & market impact models  
- Orderbook-level execution simulation  
- Walk-Forward: rolling train/valid/test windows  
- Portfolio-level accounting  
- Performance analytics

---

### **6. Production-Ready Deployment**

- Config-driven workflow  
- CLI & Makefile integration  
- Real-time data stream handler  
- Live trading interface (exchange/broker API)

---

## ⚙ Installation

```bash
# Create environment
python3 -m venv .venv
source .venv/bin/activate
````
# Install dependencies
```
pip install -r requirements.txt
```
🔧 Configuration

All experiments are driven by YAML configs:
```
config/
  data_config.yaml        # How to load & clean data
  model_config.yaml       # Model structure + parameters
  strategy_config.yaml    # Strategy parameters
  backtest_config.yaml    # Time range, capital, slippage
```
Change configs → run again → get new results
无需修改代码。
# 📦 Usage
todo
# 📁 Data

All data is stored locally and excluded from git:
```data/raw/
data/processed/
data/external/
```
Large files are intentionally ignored.

test Thu Dec  4 12:57:31 CST 2025
