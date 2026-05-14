# Orderflow — Institutional-Grade Tick Data Analysis & Backtesting

**Orderflow** is a Python package for analyzing tick-by-tick trading data, building orderflow-based trading strategies, and backtesting systematic rules on microsecond-precision market data.

Built for researchers and quants working with equity index futures (ES, NQ), it provides production-grade tools for Volume Profile analysis, Cumulative Delta (CVD) tracking, Low Volume Node (LVN) detection, execution modeling, and fast tick-by-tick backtesting.

---

## Table of Contents

- [What This Is](#what-this-is)
- [Why This Exists](#why-this-exists)
- [Critical: Trade Type Convention](#critical-trade-type-convention)
- [How It Works](#how-it-works)
  - [Data Pipeline](#data-pipeline)
  - [Data Schema](#data-schema)
  - [Backtest Engine](#backtest-engine)
  - [The Two Implemented Strategies](#the-two-strategies-implemented)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Working with the Data](#working-with-the-data)
- [Backtester API Reference](#backtester-api-reference)
- [Developing New Strategies](#developing-new-strategies)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Known Limitations](#known-limitations)

---

## What This Is

Orderflow is **not** a trading bot or signal provider. It is a data and analysis framework for one specific workflow:

1. **Enrich** raw tick data with orderflow indicators (Volume Profile, CVD, LVN, auction state)
2. **Generate** mechanical entry signals from those indicators
3. **Backtest** those signals tick-by-tick on the enriched data
4. **Analyze** results broken down by regime, hour, and market condition

### Core Capabilities

**Data Ingestion & Compression**
- Load tick-by-tick trade data with bid/ask context and DOM depth
- Compress ticks into volume bars, range bars, or time bars
- Session-aware OHLC aggregation (ETH/RTH boundaries)

**Orderflow Indicators**
- **Volume Profile** — Point of Control (POC), Value Area (VA), delta per price level
- **Cumulative Delta (CVD)** — buy/sell pressure accumulation with session resets
- **Low Volume Nodes (LVN)** — KDE-based statistical outliers in the volume distribution
- **Auction Dynamics** — block aggregation, singleton detection, naked POCs, imbalance classification
- **Market State Classifier** — balance vs. imbalance detection from price location and prior session context

**Backtester**
- Tick-by-tick simulation with **Numba acceleration** and pure-Python fallback
- Slippage models: zero, fixed, uniform random, Gaussian
- Pluggable exit strategies: FixedTPSL, TrailingStop, TimeBasedExit, VolatilityExit, CVDBreakEven, Composite
- Mechanical risk management: hard TP/SL, daily loss limits, max trades per day
- Post-trade metrics: Sharpe, Sortino, Calmar, max drawdown, MAE/MFE, profit factor

**Statistical Analysis**
- Hypothesis testing: ADF, KPSS, Jarque-Bera, CUSUM
- Bootstrap confidence intervals and Montecarlo analysis
- Markov Chain regime detection and HMM fitting
- Regime-filtered breakdowns (by hour, VIX regime, GEX regime)

---

## Why This Exists

Traditional backtesting frameworks (Backtrader, VectorBT) treat the market as a black box: orders fill at the close, slippage is a guess, and you can't see whether a move was driven by a single large aggressor or random noise.

Orderflow solves this by working at **tick resolution** with **orderflow-native indicators**:
- Every trade is visible: price, size, and which side was aggressive
- Volume Profile shows where previous participants are trapped and where price has no support
- CVD shows buy/sell pressure build-up in real time
- LVN detection identifies price levels likely to pass through quickly

From the source strategy: *"Your ability to predict is zero but your ability to read is 100."*

This package gives you tools to **read** the market tick-by-tick, then proves whether your reading was profitable through rigorous backtesting.

---

## Critical: Trade Type Convention

**ES/NQ futures use a counterintuitive convention.** `TradeType` indicates which side was *aggressive*, not which instrument changed hands.

| TradeType | Aggressor | What Happened | Signal Direction |
|-----------|-----------|---------------|-----------------|
| **1** | SELL | Seller hit the bid | SHORT trigger |
| **2** | BUY | Buyer hit the ask | LONG trigger |

**Examples:**
- `TradeType=2, Volume=50` → Buyer hit 50 contracts on the ask → BUY aggression → LONG signal
- `TradeType=1, Volume=40` → Seller hit 40 contracts on the bid → SELL aggression → SHORT signal

**CVD sign convention:** `CVD = CD_Bid - CD_Ask`
- Positive CVD = buy pressure dominant
- Negative CVD = sell pressure dominant
- CVD resets at each ETH session start

Backtest engine convention:
```python
side = Side.LONG if trade_type == 2 else Side.SHORT
tick_delta = +Volume  # TradeType == 2 (buy on ask)
tick_delta = -Volume  # TradeType == 1 (sell on bid)
```

---

## How It Works

### Data Pipeline

Three types of scripts handle the workflow. Each has a single responsibility:

#### 1. Data Enrichment — Run once per month

**Script:** `orderflow/runners/runner_data_enrichment.py`

Loads raw ticks and computes all indicators. Output is the input for all strategy runners.

```
Raw Ticks (ES/202501_ES.csv)
    ↓
    • Load 1M+ ticks per month
    • Compute Volume Profile (POC, VA, LVN, KDE shape)
    • Compute Cumulative Delta (CVD, per-level delta)
    • Compute 1-minute OHLC bars (rolling)
    • Classify market state (balance/imbalance, up/down)
    • Flag big orders (aggression >= threshold contracts)
    • Identify session boundaries (ETH/RTH)
    ↓
Enriched Ticks (202501_ES.parquet)  ~35 columns, ~1.2M rows/month
```

Run it directly from the command line — it is a standalone script, not an importable module.

---

#### 2. Signal Generation + Backtest — One file per strategy, per month

**Scripts:** `runner_data_valentini_trend_following_backtest.py`, `runner_data_valentini_meanreversion_backtest.py`

```
Enriched Ticks (202501_ES.parquet)
    ↓
    • Filter by market state, LVN, big bubble, bar confirmation
    • Sort signals by Index (required for backtest engine)
    • Run tick-by-tick backtest with slippage model
    ↓
Trade Results (trades_A_LVN_202501.parquet)
    Columns: entry_price, stop_loss, tp_price, side, exit_reason, pnl, mae, mfe
```

**Memory note:** 32GB machines must process one month at a time. Explicitly `del df` and `gc.collect()` between months or the kernel crashes.

---

#### 3. Trade Analysis — Across all months

**Scripts:** `runner_trades_valentini_trend_following_analysis.py`, `runner_trades_valentini_meanreversion_analysis.py`

```
Trade Results (trades_A_LVN_2025*.parquet)
    + External Data (VIX, GEX daily)
    ↓
    • Concatenate monthly files (reset trade_id as 1..N)
    • Join VIX term structure (contango/backwardation regime)
    • Join GEX (long/short gamma regime)
    • Aggregate metrics: win rate, profit factor, Sharpe, etc.
    • Breakdown by entry hour, VIX regime, GEX regime
    ↓
Analysis Report + Visualizations
```

---

### Data Schema

The enriched tick DataFrame has 35+ columns. The most important ones:

| Column | Type | Description |
|--------|------|-------------|
| `Index` | int64 | Unique tick ID. Used by the backtest engine to match signals. |
| `Datetime` | datetime | Full timestamp, microsecond precision. |
| `Price` | float64 | Last trade price. |
| `Volume` | int64 | Contracts traded at this tick. |
| `TradeType` | int64 | **1** = Bid/SELL aggression, **2** = Ask/BUY aggression. See convention above. |
| `AskPrice`, `BidPrice` | float64 | L1 DOM at tick time. |
| `POC` | float64 | Point of Control in current session. |
| `Prev_POC` | float64 | POC from previous session. Used as TP target. Zero on first session — always filter it out. |
| `VA_Areas` | str | `"VA"` = inside Value Area, `"PO"` = on POC, `"na"` = outside (imbalance). |
| `CVD` | float64 | Cumulative buy-sell delta from session start. |
| `LVN` | int8 | `1` if price is at a Low Volume Node, else `0`. Location flag variant A. |
| `ValleysPeaks` | float64 | KDE shape: -2/-1 (valley area), 0 (undefined), 1/2 (peak area). Location flag variant B. |
| `Session_High`, `Session_Low` | float64 | Rolling high/low from ETH session start. |
| `current_bar_datetime` | datetime | Open timestamp of the 1-minute bar containing this tick. |
| `current_bar_open/high/low/close` | float64 | OHLC of the current 1-minute bar. |
| `next_bar_open` | float64 | **Entry price** for signals — the open of the next 1-minute bar. |
| `market_state` | str | `"balance"`, `"imbalance_up"`, `"imbalance_down"`, `"imbalance_no_target"`. |
| `SessionType` | str | `"RTH"` (08:30–16:00 CT) or `"ETH"` (extended hours). |

---

### Backtest Engine

The engine processes ticks as a state machine. At each tick it checks, in order:

1. Has a new signal arrived? → open position
2. Is stop loss hit? → close as LOSS
3. Is take profit hit? → close as WIN
4. Is session end? → close as TIME_EXIT
5. Does CVD condition trigger break-even? → move stop to break-even
6. Does exit strategy say exit? → close with strategy reason

**Critical design decisions:**
- **No lookahead:** all exit checks evaluate at tick time, not retroactively
- **Signal ordering:** engine consumes signals sequentially by `Index`; signals MUST be sorted ascending before passing to the engine
- **Entry price:** always `next_bar_open` (the bar after the signal bar). Never the trigger price.
- **Signal pointer desync:** when any position closes mid-session, the pointer must skip all signals that occurred during the trade — otherwise subsequent trades enter with the wrong direction

---

### The Two Strategies (Implemented)

#### Valentini Trend Following (A_LVN variant)

**Backtested:** Jan 2025 – Feb 2026. **Best PF: 3.61** (out-of-sample, hour filter applied).

**Three-step mechanical entry:**
1. **Market State** — Price outside Value Area AND aligned with Prev_POC direction (`imbalance_up` or `imbalance_down`)
2. **Location** — Price at a Low Volume Node (`LVN == 1`)
3. **Trigger** — Big aggressive order (≥10 contracts on ES) in the direction of imbalance

**Exit:** Take profit at Prev_POC. Stop loss 2–3 ticks from trigger price. CVD break-even when MFE ≥ 2 ticks AND delta turns favorable.

**Best hours:** 9am and 2pm CT. Hours 10, 11, 13 are systematically negative.

---

#### Valentini Mean Reversion (B_valley + hour 10)

**Backtested:** Jan 2025 – Dec 2025 (in-sample), Jan 2026 (out-of-sample). **PF 3.78 IS / 3.61 OOS.**

Opposite structure to trend following:
1. **Market State** — Price outside Value Area but **opposing** Prev_POC direction
2. **Location** — Valley in KDE shape (`ValleysPeaks <= -1`)
3. **Trigger** — Aggressive print in the direction of dislocation

**Exit:** Take profit at **current session POC** (not Prev_POC). Stop loss 2 ticks beyond trigger price.

Fades extremes where the daily structure does not support the move.

---

## Installation

**Requirements:** Python 3.9+, 8GB+ RAM (16GB+ recommended for month-long backtests)

```bash
git clone https://github.com/andreaferrante/orderflow.git
cd orderflow

# Editable install — required. Never edit site-packages directly.
pip install -e .

# Verify
python -c "import orderflow; print(orderflow.__version__)"
```

**Core dependencies** (installed automatically):
- **Polars** — fast operations on large tick datasets
- **Pandas** — analysis, join operations, CSV I/O
- **Numba** — JIT compilation of the backtest core loop
- **Scikit-learn** — HMM regime detection, bootstrap
- **hmmlearn** — Hidden Markov Model fitting
- **Plotly / Matplotlib** — interactive and static charts

---

## Quick Start

### 1. Run data enrichment (once per month)

```bash
# Edit paths inside the script, then run directly
python orderflow/runners/runner_data_enrichment.py
# Output: sources/ES/parquet/202501_ES.parquet
```

### 2. Run a backtest

The backtest runners are standalone scripts. Run them directly:

```bash
python orderflow/runners/runner_data_valentini_trend_following_backtest.py
# Output: sources/ES/parquet/trades_A_LVN_202501.parquet
```

Or use the `BacktestEngine` API directly in your own code:

```python
import pandas as pd
from orderflow.backtester import BacktestEngine, FixedTPSLExit
from orderflow.backtester.execution import SlippageModel, SlippageMode

# Load enriched ticks (Pandas — engine requires Pandas DataFrame)
df = pd.read_parquet('sources/ES/parquet/202501_ES.parquet')

# Build signals DataFrame — must have Index and TradeType columns
# (Index matches the tick row to enter on; TradeType: 1=SHORT, 2=LONG)
signals = df[
    (df['SessionType'] == 'RTH') &
    (df['Prev_POC'] > 0) &
    (df['market_state'].str.contains('imbalance')) &
    (df['LVN'] == 1) &
    (df['Volume'] >= 10)
][['Index', 'TradeType']].copy()

signals = signals.sort_values('Index').reset_index(drop=True)  # REQUIRED

# Configure engine
engine = BacktestEngine(
    tick_size=0.25,
    tick_value=12.50,
    commission=0.9,
    n_contracts=1,
    slippage_model=SlippageModel(mode=SlippageMode.UNIFORM, max_ticks=2, seed=42),
)

# Run backtest with fixed TP/SL (in ticks)
result = engine.run(
    data=df,
    signals=signals,
    tp_ticks=50,
    sl_ticks=8,
)

print(result.metrics.summary())
print(result.trades.head())
```

### 3. Analyze trades by regime

```python
import pandas as pd

# Load all monthly trade files
import glob
trades = pd.concat([pd.read_parquet(f) for f in glob.glob('sources/ES/parquet/trades_A_LVN_*.parquet')])
trades['trade_id'] = range(1, len(trades) + 1)  # reset IDs

# Load external regime data
vix = pd.read_csv('sources/VIX/vix.csv', parse_dates=['date'])
gex = pd.read_csv('sources/GEX/dix.csv', parse_dates=['date'])

# Merge (use prev-day values — shift(1) already applied in source files)
trades['date'] = pd.to_datetime(trades['entry_datetime']).dt.date
trades = trades.merge(vix[['date', 'is_contango_prev']], on='date')
trades = trades.merge(gex[['date', 'is_long_gamma_prev']], on='date')

# Breakdown by VIX regime
print(trades.groupby('is_contango_prev')['pnl'].sum())

# Breakdown by hour
print(trades.groupby(pd.to_datetime(trades['entry_datetime']).dt.hour)['pnl'].sum())
```

---

## Architecture

```
orderflow/
├── backtester/              # Tick-by-tick engine + exits + metrics
│   ├── engine.py            # BacktestEngine, BacktestResult
│   ├── exits.py             # Exit strategies: FixedTPSL, Trailing, Dynamic, CVDBreakEven
│   ├── execution.py         # SlippageModel, SlippageMode, FillSimulator
│   ├── risk.py              # RiskManager (hard TP/SL, daily limits)
│   └── metrics.py           # PerformanceMetrics, Sharpe/Sortino/Calmar
│
├── compressor/              # Tick aggregation
│   └── compressor.py        # Volume bars, range bars, time bars (Polars)
│
├── stats/                   # Statistical analysis & regime detection
│   ├── stats.py             # Risk metrics
│   ├── hypothesis.py        # ADF, KPSS, Jarque-Bera, CUSUM
│   ├── montecarlo.py        # Bootstrap, confidence intervals
│   └── markov.py            # Markov chains, HMM regime detection
│
├── volume_profile.py        # POC, VAH/VAL, dynamic CVD, session delta
├── volume_profile_kde.py    # Gaussian KDE, LVN/peak detection (Numba)
├── auctions.py              # Block aggregation, auction theory
├── markov.py                # MarkovChainPredictor, AdaptiveMarkovChainPredictor
├── vwap.py                  # VWAP + AVWAP with std dev bands
├── footprint.py             # Footprint chart utilities
├── dom.py                   # DOM shape analysis
├── sc.py                    # Data cleaning (Sierra Chart format)
├── ohlc.py                  # OHLC bar utilities
│
├── runners/                 # Standalone workflow scripts (NOT importable as a package)
│   ├── runner_data_enrichment.py
│   ├── runner_data_valentini_trend_following_backtest.py
│   ├── runner_data_valentini_meanreversion_backtest.py
│   ├── runner_trades_valentini_trend_following_analysis.py
│   └── runner_trades_valentini_meanreversion_analysis.py
│
└── test/                    # Unit tests (pytest)
```

### Design Patterns

- **Polars for large data, Pandas for analysis:** Enrichment and compression use Polars; backtest engine consumes Pandas DataFrames
- **Numba acceleration:** Hot loops in the backtester use JIT compilation with a pure-Python fallback
- **No lookahead bias:** All rolling operations are strictly causal
- **Editable install:** Always `pip install -e .`; the `runners/` scripts import from `orderflow` at the editable path

---

## Working with the Data

### Load and filter enriched ticks

```python
import polars as pl

df = pl.read_parquet('sources/ES/parquet/202501_ES.parquet')
print(df.shape)   # ~(1_200_000, 35)
print(df.columns)

# RTH only, skip first session (no Prev_POC yet)
df_rth = df.filter(
    (pl.col('SessionType') == 'RTH') &
    (pl.col('Prev_POC') > 0)
)

# All ticks in one 1-minute bar
bar = df.filter(pl.col('current_bar_datetime') == pl.lit('2025-01-15 09:31:00').str.strptime(pl.Datetime))
print(bar.select(['Index', 'Price', 'Volume', 'TradeType', 'LVN', 'POC']))
```

### Finding Low Volume Nodes

```python
# Variant A: explicit LVN flag
lvn_ticks = df.filter(pl.col('LVN') == 1)

# Variant B: KDE valley shape
valley_ticks = df.filter(pl.col('ValleysPeaks') <= -1)

# Combined: valley + imbalance + big bubble
candidates = df.filter(
    (pl.col('ValleysPeaks') <= -1) &
    (pl.col('market_state').str.contains('imbalance')) &
    (pl.col('Volume') >= 10) &
    (pl.col('SessionType') == 'RTH')
)
print(f"Signal candidates: {candidates.shape[0]}")
```

### Joining External Data (VIX, GEX)

```python
import pandas as pd

trades = pd.read_parquet('sources/ES/parquet/trades_A_LVN_202501.parquet')

# VIX term structure
vix = pd.read_csv('sources/VIX/vix_data.csv', parse_dates=['date'])
# is_contango_prev must use .shift(1) — day T gets day T-1 value

# GEX (SqueezeMetrics dix.csv)
gex = pd.read_csv('sources/GEX/dix.csv', parse_dates=['date'])
# is_long_gamma_prev must use .shift(1) — same rule

trades['date'] = pd.to_datetime(trades['entry_datetime']).dt.date
trades = trades.merge(vix[['date', 'is_contango_prev']], on='date')
trades = trades.merge(gex[['date', 'is_long_gamma_prev']], on='date')

# For trend following: backwardation + short gamma = best conditions
best = trades[(trades['is_contango_prev'] == False) & (trades['is_long_gamma_prev'] == False)]
print(f"Best subset: {len(best)} trades")
```

---

## Backtester API Reference

### BacktestEngine

```python
from orderflow.backtester import BacktestEngine
from orderflow.backtester.execution import SlippageModel, SlippageMode

engine = BacktestEngine(
    tick_size=0.25,        # ES minimum price increment
    tick_value=12.50,      # $ value per tick ($0.25 * $50/point = $12.50)
    commission=0.9,        # $ per side per contract
    n_contracts=1,
    slippage_model=SlippageModel(mode=SlippageMode.UNIFORM, max_ticks=2, seed=42),
    progress_bar=False,
)

result = engine.run(
    data=df,               # Pandas DataFrame — must have: Index, Datetime, Price, Date, Time, SessionType
    signals=signals_df,    # Pandas DataFrame — must have: Index (tick to enter on), TradeType (1=SHORT, 2=LONG)
    tp_ticks=50,           # Take-profit distance in ticks (convenience parameter)
    sl_ticks=8,            # Stop-loss distance in ticks
    exit_strategy=None,    # Optional: plug in a custom BaseExitStrategy instead
    risk_manager=None,     # Optional: full RiskManager object overrides tp_ticks/sl_ticks
    indicator_columns=['CVD'],  # Columns from `data` passed to exit strategy
)

print(result.metrics.summary())
print(result.trades)       # Pandas DataFrame of closed trades
```

### SlippageMode Options

```python
from orderflow.backtester.execution import SlippageMode

SlippageMode.ZERO       # No slippage (default)
SlippageMode.FIXED      # Always N ticks
SlippageMode.UNIFORM    # Random uniform 0..max_ticks
SlippageMode.GAUSSIAN   # Gaussian with mean/std params
```

### Exit Strategies

```python
from orderflow.backtester import (
    FixedTPSLExit,      # Fixed stop/target in ticks
    TrailingStopExit,   # Trailing stop behind favorable price
    TimeBasedExit,      # Exit after N bars regardless of P&L
    VolatilityExit,     # Exit when volatility drops below threshold
    DynamicTPSLExit,    # ATR-based adaptive stops
    CVDBreakEvenExit,   # CVD-triggered break-even
    CompositeExit,      # Combine strategies (first hit wins)
)

# Fixed stop/target (in ticks)
FixedTPSLExit(tp=50, sl=8)

# Trailing stop
TrailingStopExit(trailing_ticks=15, initial_sl=8)

# Time-based max hold
TimeBasedExit(max_bars=30)

# Composite: exit on whichever condition hits first
CompositeExit([
    FixedTPSLExit(tp=50, sl=8),
    TimeBasedExit(max_bars=30),
])
```

### PerformanceMetrics

```python
m = result.metrics

print(m.summary())          # Pretty-printed all metrics

# Individual values
m.win_rate                  # float: 0.52
m.profit_factor             # float: 2.8
m.max_drawdown              # float: -4200.0 (in dollars)
m.sharpe_ratio              # float
m.sortino_ratio             # float
m.calmar_ratio              # float
m.expectancy                # float: average $ per trade
```

---

## Developing New Strategies

### Step 1: Write a strategy spec

Create `docs/strategies/<strategy_name>.md` with:
- Source (paper, video, trader)
- Core concept (3–5 sentences)
- Entry rules (market state filter, location filter, trigger)
- Exit rules (TP target, SL logic, break-even condition)
- Backtest results (IS/OOS split)
- Known issues and open questions

### Step 2: Generate signals and run the backtest

Signals are a Pandas DataFrame with at minimum `Index` (tick to enter on) and `TradeType` (1=SHORT, 2=LONG). The engine enters at the open of the next bar after the signal tick.

```python
import pandas as pd
import polars as pl
from orderflow.backtester import BacktestEngine
from orderflow.backtester.execution import SlippageModel, SlippageMode

# Load enriched ticks as Polars for fast filtering
df_pl = pl.read_parquet('sources/ES/parquet/202501_ES.parquet')

# Generate signals: filter to qualifying ticks
signals_pl = df_pl.filter(
    (pl.col('SessionType') == 'RTH') &
    (pl.col('Prev_POC') > 0) &
    (pl.col('market_state').str.contains('imbalance')) &
    (pl.col('LVN') == 1) &
    (pl.col('Volume') >= 10)
).select(['Index', 'TradeType'])

# Convert to Pandas, sort by Index (REQUIRED)
signals = signals_pl.to_pandas().sort_values('Index').reset_index(drop=True)

# Convert tick data to Pandas for the engine
df = df_pl.to_pandas()

# Run backtest
engine = BacktestEngine(
    tick_size=0.25,
    tick_value=12.50,
    commission=0.9,
    slippage_model=SlippageModel(mode=SlippageMode.UNIFORM, max_ticks=2, seed=42),
    progress_bar=False,
)
result = engine.run(data=df, signals=signals, tp_ticks=50, sl_ticks=8)

# Save trades
result.trades.to_parquet('sources/ES/parquet/trades_my_strategy_202501.parquet')

print(result.metrics.summary())
```

### Step 3: Multi-month loop (memory-safe)

```python
import gc
import glob
import pandas as pd

all_trades = []
months = ['202501', '202502', '202503', '202504', '202505']

for month in months:
    df_pl = pl.read_parquet(f'sources/ES/parquet/{month}_ES.parquet')
    signals = build_signals(df_pl)  # your function
    df = df_pl.to_pandas()

    result = engine.run(data=df, signals=signals, tp_ticks=50, sl_ticks=8)
    all_trades.append(result.trades)

    # CRITICAL: explicit cleanup or kernel crashes on 32GB machines
    del df_pl, df, signals, result
    gc.collect()

trades = pd.concat(all_trades).reset_index(drop=True)
trades['trade_id'] = range(1, len(trades) + 1)
```

### Step 4: Validate

Split Jan–Sep as in-sample, Oct–Dec as out-of-sample.

Minimum bar for a strategy to pass:
- Profit Factor ≥ 1.5 (both IS and OOS)
- Win rate ≥ 45%
- Max drawdown < 15% of total gross profit
- No single month driving most of the P&L

---

## Testing

```bash
# All tests
pytest

# Specific file
pytest orderflow/test/test_volume_profile.py -v

# With coverage
pytest --cov=orderflow --cov-report=html

# Backtester only
pytest orderflow/test/ -k backtester -v
```

---

## Troubleshooting

### Kernel crash during backtest (Windows, 32GB RAM)

Each monthly parquet file is ~1.2M ticks. Loading multiple months plus intermediate DataFrames exhausts 32GB.

**Fix:** process one month at a time and clean up aggressively:
```python
del df_pl, df, signals, result
import gc; gc.collect()
```

### Wrong trade directions after time exit

**Cause:** signal pointer desync. When a position closes via time exit, the engine must skip all signals that occurred during that trade. Without this, the next trade's `TradeType` is wrong.

**Fix:** already patched in `engine.py`. If you see wrong directions, verify you are on the latest version.

### All trades enter in the same direction

**Cause:** signals DataFrame not sorted by `Index`. The engine reads signals sequentially by position in the array, not by timestamp lookup.

**Fix:**
```python
signals = signals.sort_values('Index').reset_index(drop=True)
```

### NaN in TP target / zero Prev_POC

**Cause:** first session in dataset has no prior session, so `Prev_POC = 0`.

**Fix:**
```python
df = df.filter(pl.col('Prev_POC') > 0)  # Polars
# or
df = df[df['Prev_POC'] > 0]             # Pandas
```

### VIX/GEX lookahead bias

**Cause:** merging today's trades with today's VIX close. VIX is published end-of-day; trades happen intraday.

**Fix:** always use `shift(1)` when constructing regime columns:
```python
vix['is_contango_prev'] = vix['is_contango'].shift(1)
```

---

## Development

### Pre-commit hooks

Commits must use conventional format: `type(scope): description`

Valid types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`

Example: `feat(backtester): add CVD break-even exit strategy`

### Code style

- Black formatting (line length 88)
- Type hints required for public functions
- Google-style docstrings for modules and classes

### Contributing

```bash
git checkout -b feat/your-feature
# Write tests, implement, verify pytest passes
git commit -m "feat(module): description"
# Open pull request
```

---

## Known Limitations

1. **Tick data quality** — Raw feed may drop ticks during CPI, FOMC, or gap opens. Estimated error ~2% of sessions. Backtest results contain this measurement error.

2. **Memory constraints** — 32GB machines can't load a full year of ticks at once. Workaround: per-month loop + concatenation.

3. **L1 DOM only** — Data includes best bid/ask only. No order book depth (L2+). Advanced order book imbalance analysis is not supported.

4. **Single instrument** — Currently ES-only. Infrastructure supports any tick data with `TradeType`; NQ and /YM support is planned.

5. **Session-forced exit** — All positions close at 15:00 CT. No overnight holding. This is intentional (part of the Valentini rules) but limits strategies that need wider holding windows.

---

## License

MIT License. See `LICENCE` file.

---

## Support

- **Issues:** [GitHub Issues](https://github.com/andreaferrante/orderflow/issues)
- **Strategy specs:** `docs/strategies/` — detailed rule documentation for each implemented strategy
- **Backtester API:** `orderflow/backtester/README.md` and in-code docstrings

---

## Acknowledgments

Strategy rules extracted from:
- **Valentini Trend Following / Mean Reversion** — Fabio Valentini, "Chart Fanatics" podcast
- **Market Microstructure theory** — James Dalton, *Markets in Profile*
- **Auction Market Theory** — Peter Steidlmayer
