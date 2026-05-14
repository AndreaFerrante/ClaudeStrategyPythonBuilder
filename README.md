# Orderflow — Institutional-Grade Tick Data Analysis & Backtesting

**Orderflow** is a professional Python package for analyzing tick-by-tick trading data, building orderflow-based trading strategies, and backtesting systematic rules on microsecond-precision market data.

Built for researchers and quants who work with high-frequency data from equity index futures (ES, NQ), Orderflow provides production-grade tools for Volume Profile analysis, Cumulative Delta (CVD) tracking, Low Volume Node (LVN) detection, execution modeling, and fast tick-by-tick backtesting.

---

## What This Is

### A Toolkit for Orderflow-Driven Research

Orderflow is **not** a trading bot or a signal provider. It's a data and analysis framework designed for one specific workflow:

1. **Load** tick-by-tick data (price, volume, trade aggression side)
2. **Enrich** with orderflow indicators (Volume Profile, CVD, LVN, auction dynamics)
3. **Backtest** mechanical trading rules on that enriched data
4. **Analyze** results by regime, market condition, and time window

### Core Capabilities

#### Data Ingestion & Compression
- Load tick-by-tick trade data with bid/ask context and DOM depth
- Compress ticks into volume bars, range bars, time bars
- OHLC aggregation with session-aware resets
- Tick-level precision for microsecond-scale ordering

#### Orderflow Indicators
- **Volume Profile** — Point of Control (POC), Value Area (VA), delta per price level
- **Cumulative Delta (CVD)** — buy/sell pressure accumulation with session resets
- **Low Volume Nodes (LVN)** — statistical outliers in the volume distribution using KDE
- **Auction Dynamics** — block aggregation, singleton detection, naked POCs, imbalance classification
- **Market State Classifier** — balance vs imbalance detection based on price location and prior context
- **Markov Chain Analysis** — regime detection via HMM and state prediction

#### Backtester
- Tick-by-tick simulation with **Numba acceleration** and pure-Python fallback
- Fill simulation with slippage models (fixed %, random, market impact)
- Pluggable exit strategies (FixedTPSL, TrailingStop, TimeBasedExit, VolatilityExit, CVDBreakEven)
- Mechanical risk management (hard TP/SL stops, daily loss limits, trade count limits)
- Post-trade metrics: Sharpe, Sortino, Calmar, max drawdown, MAE/MFE, win rate, profit factor

#### Statistical Analysis
- Hypothesis testing: ADF, KPSS, Jarque-Bera, CUSUM
- Bootstrap confidence intervals
- Montecarlo analysis with survivorship bias correction
- Skewness and kurtosis analysis
- Regime-filtered breakdowns (by hour, VIX regime, GEX regime, etc.)

---

## Why This Exists

### The Problem It Solves

Traditional backtesting frameworks (Backtrader, VectorBT, etc.) treat the market as a black box:
- Order fills happen at the close price; slippage is a guess
- You can't see the difference between a breakout above Value Area (trend signal) and a fake to the upside inside VA (trap)
- You lose microsecond-scale context — who was aggressing? Was it a single trader or institutional absorption?
- Testing on daily bars captures maybe 5 trading days per month; hidden tail risk lives in the 21,000 ticks you skip

Orderflow solves this by:
- Working at **tick resolution** — you see every trade that happened
- Providing **orderflow-native indicators** — Volume Profile, CVD, LVN — that professional traders read from their charts
- Respecting **market microstructure** — distinguishing real aggression (big orders on the ask/bid) from noise (small prints)
- Enabling **session-aware analysis** — ETH/RTH boundaries, session-specific POC targets, intraday regime shifts

### Who This Is For

1. **Systematic traders** building rules-based strategies around orderflow patterns
2. **Researchers** studying market microstructure, auction theory, or regime detection
3. **Quants** backtesting multiple strategy variants on large historical datasets
4. **Portfolio managers** wanting to understand the texture of intraday moves (not just OHLC)

### Philosophy

From the source strategy ("Valentini Trend Following"):

> *"Your ability to predict is zero but your ability to read is 100."*

This package embodies that philosophy: it gives you the tools to **read** the market — to see what really happened tick-by-tick — not to predict the future. The backtest proves whether your reading was profitable. The regime analysis shows you *when* your reading was profitable (only in certain market states?).

---

## How It Works

### Data Pipeline

Orderflow separates concerns into **three types of scripts**, each with a single responsibility:

#### 1. Data Enrichment (Once Per Month)

**File:** `runner_data_enrichment.py`

Runs when new tick data arrives. Loads raw ticks and computes all orderflow indicators once:

```
Raw Ticks (ES/202501_ES.csv)
    ↓
    • Load 1M+ ticks per month
    • Compute Volume Profile (POC, VA, LVN, KDE shape)
    • Compute Cumulative Delta (CVD, per-level delta)
    • Compute 1-minute OHLC bars (rolling)
    • Classify market state (balance/imbalance, up/down)
    • Flag big orders (aggression >10 contracts)
    • Identify session boundaries (ETH/RTH)
    ↓
Enriched Ticks (202501_ES.parquet)
```

**Output schema:** 35+ columns including Index, Price, Volume, TradeType, POC, CVD, Session_High, Session_Low, next_bar_open (entry price), etc.

**Why separate?** Tick-level computation is expensive. Computing once and caching as Parquet avoids recomputation. The enriched file becomes the input to all strategy runners.

---

#### 2. Signal Generation + Backtest (Per Strategy, Per Month)

**Files:** `runner_data_valentini_trend_following_backtest.py`, `runner_data_valentini_meanreversion_backtest.py`

Loads enriched ticks and executes:

```
Enriched Ticks (202501_ES.parquet)
    ↓
    • Generate signals (filter by market state, LVN, big bubble, bar confirmation)
    • Sort signals by Index (required for engine)
    • Run tick-by-tick backtest with slippage model
    • Track every trade: entry, stop loss, take profit, exit reason
    ↓
Trade Results (trades_variant_202501.parquet)
    Columns: entry_price, stop_loss, tp_price, side, exit_reason, pnl, mae, mfe, entry_index, exit_index
```

**Memory management:** 32GB limit on Windows. Process one month at a time; explicitly `del df` and `gc.collect()` between months.

---

#### 3. Trade Analysis (Across All Months)

**Files:** `runner_trades_valentini_trend_following_analysis.py`, `runner_trades_valentini_meanreversion_analysis.py`

Aggregates all monthly trade files and computes full-period statistics:

```
Trade Results (trades_A_LVN_2025*.parquet + trades_B_valley_2025*.parquet)
    +
External Data (VIX, GEX)
    ↓
    • Concatenate monthly files (recalculate trade_id as 1..N)
    • Join daily VIX (contango/backwardation regime)
    • Join daily GEX (long/short gamma regime)
    • Compute aggregate metrics (win rate, profit factor, Sharpe, etc.)
    • Breakdown by hour, VIX regime, GEX regime
    • Find profitable subsets
    ↓
Analysis Report + Visualizations
    • CSV outputs: hourly breakdown, regime breakdown, MAE/MFE distributions
    • HTML plots: equity curve, win/loss distribution, drawdown chart
```

---

### Data Schema

The enriched tick DataFrame (produced by data enrichment runner) contains:

| Column | Type | Purpose |
|--------|------|---------|
| `Index` | int64 | Unique tick ID. Used by backtest engine to match signals. |
| `Datetime` | datetime | Full timestamp, microsecond precision. |
| `Price` | float64 | Last trade price. |
| `Volume` | int64 | Contracts at this tick. |
| `TradeType` | int64 | **1** = Bid/SELL aggression, **2** = Ask/BUY aggression |
| `AskPrice`, `BidPrice` | float64 | L1 DOM at tick time. |
| `POC` | float64 | Point of Control in current session. |
| `Prev_POC` | float64 | POC from previous session (TP target). Zero = first session (filter it). |
| `VA_Areas` | str | `"VA"`, `"PO"`, or `"na"` (outside VA = imbalance). |
| `CVD` | float64 | Cumulative buy-sell delta from session start. |
| `LVN` | int8 | 1 if low volume node, else 0. |
| `ValleysPeaks` | float64 | KDE shape: -2/-1 (valley), 0 (undefined), 1/2 (peak). |
| `Session_High`, `Session_Low` | float64 | Rolling high/low from session start. |
| `current_bar_*` | float64 | OHLC of the 1-min bar containing this tick. |
| `next_bar_open` | float64 | Entry price for signals (open of the next bar). |
| `market_state` | str | `"balance"`, `"imbalance_up"`, `"imbalance_down"`, `"imbalance_no_target"`. |
| `SessionType` | str | `"RTH"` (regular hours) or `"ETH"` (extended hours). |

---

### Backtest Engine

The tick-by-tick backtest engine operates as a state machine:

```python
for i in range(len(ticks)):
    current_price = ticks[i]
    
    # Check if next signal matches this tick
    if signal_ptr < len(signals) and signal_timestamp[signal_ptr] == current_timestamp[i]:
        # Open new position
        position = {side, entry_price: next_bar_open[signal], stop_loss: ..., tp: ...}
        signal_ptr += 1
    
    if position:
        # Check exit conditions in priority order
        if price <= stop_loss:
            exit(side=LOSS, reason=STOP_LOSS)
        elif price >= take_profit:
            exit(side=WIN, reason=TAKE_PROFIT)
        elif current_time >= session_end:
            exit(side=MIXED, reason=SESSION_CLOSE)
        elif cvd_break_even_condition:
            update_stop_loss_to_break_even()
        # ... other exit conditions
```

**Key design decisions:**
- **No lookahead:** exit conditions are evaluated at tick time, not retconned
- **Signal ordering:** signals processed sequentially by their `Index` (tick ID), not timestamp
- **Fill simulation:** entry price = `next_bar_open` (the open after the signal bar); fills use slippage model
- **Priority exits:** stop loss checked before take profit (closer exit wins)
- **Post-trade metrics:** MAE (max adverse excursion), MFE (max favorable excursion), Sharpe/Sortino on P&L

---

### The Two Strategies (Implemented)

#### Valentini Trend Following (A_LVN variant)
**Status:** Backtested Jan 2025 – Feb 2026. Best Profit Factor: **3.61** (out-of-sample).

Three-step mechanical entry rule:
1. **Market State:** Price outside Value Area (imbalance) AND aligned with previous POC direction
2. **Location:** At a Low Volume Node (statistical outlier in volume distribution)
3. **Trigger:** Big aggressive order (10+ contracts on ES) in the direction of imbalance

Exit: Take profit at Prev_POC (previous session's POC). Stop loss 2–3 ticks from trigger. CVD break-even when MFE >= 2 ticks AND delta turns favorable.

**Best hours:** 9am, 2pm CT. Other hours unprofitable. **Best variant:** A_LVN (more selective than B_valley).

#### Valentini Mean Reversion (B_valley + hour 10 variant)
**Status:** Validated Jan 2025 – Dec 2025 in-sample, Jan 2026 out-of-sample. PF **3.78** (IS) / **3.61** (OOS).

Opposite of trend following:
1. **Market State:** Price outside Value Area BUT opposing the Prev_POC
2. **Location:** Valley (KDE shape <= -1)
3. **Trigger:** Aggressive single print (low volume) in the direction of the dislocation

Exit: Take profit at **current session POC** (not Prev_POC). Stop loss 2 ticks beyond trigger price.

**Why it works:** Fades extremes when the market is pricing in a move the daily structure doesn't support.

---

## Installation

### Requirements
- Python 3.9+
- 8GB+ RAM (16GB+ for month-long backtests)
- Polars 0.19+, Pandas 1.5+, Numba 0.57+

### Setup

```bash
# Clone the repository
git clone https://github.com/andreaferrante/orderflow.git
cd orderflow

# Install in editable mode (required — do not edit site-packages)
pip install -e .

# Verify installation
python -c "import orderflow; print(orderflow.__version__)"
```

### Dependencies

Core dependencies installed automatically:
- **Polars** — fast DataFrame operations on large tick datasets
- **Pandas** — analysis, join operations, CSV I/O
- **Numpy** — numerical ops
- **Numba** — JIT compilation of the backtest core loop
- **Scikit-learn** — HMM regime detection, bootstrap
- **Plotly** — interactive charts
- **Matplotlib** — static plots
- **hmmlearn** — Hidden Markov Model fitting

---

## Quick Start

### 1. Load and Enrich Tick Data

```python
from orderflow.runners import runner_data_enrichment

# Loads raw ES ticks from sources/ES/ticks/202501_ES.csv
# Computes all orderflow indicators
# Saves to sources/ES/parquet/202501_ES.parquet
enriched_df = runner_data_enrichment.run(month="202501")
print(enriched_df[['Index', 'Price', 'Volume', 'POC', 'CVD', 'LVN']].head())
```

### 2. Run a Backtest

```python
from orderflow.backtester import BacktestEngine, FixedTPSLExit
from orderflow.runners import runner_data_valentini_trend_following_backtest as tv

# Load enriched ticks
ticks_df = ... # e.g., from Parquet

# Generate signals (market state + LVN + big bubble filter)
signals_df = tv.generate_signals(ticks_df, variant='A_LVN', big_order_threshold=10)

# Run backtest
engine = BacktestEngine(tick_size=0.25, tick_value=12.5)
result = engine.run(
    ticks=ticks_df,
    signals=signals_df,
    exit_strategy=FixedTPSLExit(tp_ticks=50, sl_ticks=8)
)

# View results
print(result.metrics.summary())
# Win Rate: 52%, PF: 2.8, Max DD: -$4,200, Sharpe: 1.23
```

### 3. Analyze Trades by Regime

```python
from orderflow.stats import compute_regime_breakdown

trades_df = result.trades  # Output from backtest

# Join VIX regime (contango/backwardation from prior day)
trades_df = trades_df.merge(vix_regime, on='date', how='left')

# Breakdown by market condition
by_hour = trades_df.groupby('hour').agg({'pnl': 'sum', 'win_rate': 'mean'})
by_vix = trades_df.groupby('is_contango_prev').agg({'pnl': 'sum', 'profit_factor': 'mean'})

print(by_hour)
print(by_vix)
```

---

## Architecture

### Module Organization

```
orderflow/
├── backtester/              # Tick-by-tick engine + exits + metrics
│   ├── engine.py            # BacktestEngine, run loop
│   ├── exits.py             # Exit strategies (FixedTPSL, Trailing, Dynamic, CVDBreakEven)
│   ├── execution.py         # FillSimulator, SlippageModel
│   ├── risk.py              # RiskManager (hard TP/SL, daily limits)
│   └── metrics.py           # PerformanceMetrics, Sharpe/Sortino/Calmar
│
├── compressor/              # Tick aggregation
│   └── compressor.py        # VolumeBar, RangeBar, TimeBar
│
├── stats/                   # Statistical analysis & regime detection
│   ├── stats.py             # HMM, Markov chains, bootstrap, hypothesis tests
│   ├── montecarlo.py        # Confidence intervals, survivorship correction
│   └── markov_utilities.py  # State transition matrices
│
├── volume_profile.py        # POC, VA, LVN (KDE-based)
├── auctions.py              # Block aggregation, naked POCs, auction theory
├── markov.py                # Markov chain predictor, regime detection
├── vwap.py                  # VWAP, AVWAP + std dev bands
├── footprint.py             # Footprint chart utilities
├── sc.py                    # Data cleaning (Sierra Chart format)
├── ohlc.py                  # Bar utilities
├── viz.py                   # Matplotlib/Plotly helpers
│
├── runners/                 # Standalone workflow scripts
│   ├── runner_data_enrichment.py
│   ├── runner_data_valentini_trend_following_backtest.py
│   ├── runner_data_valentini_meanreversion_backtest.py
│   ├── runner_trades_valentini_trend_following_analysis.py
│   ├── runner_trades_valentini_meanreversion_analysis.py
│   ├── runner.py            # Generic backtest harness
│   └── consistency_data_validator.py
│
└── test/                    # Unit tests (pytest)
    └── test_*.py
```

### Design Patterns

1. **Dual DataFrame Support:** Most functions accept both Pandas and Polars, but Polars is preferred for large datasets (faster, lower memory)
2. **Numba Acceleration:** Hot loops in backtester have Numba JIT compilation with pure-Python fallback
3. **No Lookahead Bias:** All rolling operations strictly causal (no future data)
4. **Editable Install:** Always `pip install -e .`; never edit site-packages
5. **Conventional Commits:** Commits use `type(scope): description` format (enforced by pre-commit hook)

---

## Trade Type Convention (Critical)

**ES/NQ futures use a counterintuitive convention:** TradeType indicates the *side that was aggressive*, not the trade direction.

| TradeType | Market Aggressor | Entry Signal |
|-----------|------------------|--------------|
| **1** | SELL (aggressive seller on bid) | SHORT setup |
| **2** | BUY (aggressive buyer on ask) | LONG setup |

Example:
- **TradeType=2, Volume=50 contracts**: Buyer hit 50 contracts on the ask → aggressive buy → LONG signal
- **TradeType=1, Volume=40 contracts**: Seller hit 40 contracts on the bid → aggressive sell → SHORT signal

CVD convention: `CVD = CD_Bid - CD_Ask` (positive = accumulation of buy pressure). Positive CVD + rising price = trend confirmation.

---

## Working with the Data

### Import and Inspect

```python
import polars as pl
import pandas as pd

# Load enriched tick data
df = pl.read_parquet('sources/ES/parquet/202501_ES.parquet')
print(df.shape)  # (1,200,000, 35)
print(df.columns)

# Filter to RTH only, skip first session
df_rth = df.filter(
    (pl.col('SessionType') == 'RTH') &
    (pl.col('Prev_POC') > 0)
)

# View a single bar's ticks
bar_ticks = df.filter(pl.col('current_bar_datetime') == '2025-01-15 09:31:00')
print(bar_ticks.select(['Index', 'Price', 'Volume', 'TradeType', 'LVN', 'POC']))
```

### Finding Low Volume Nodes

```python
# Variant A: Explicit LVN flag
lvn_ticks = df.filter(pl.col('LVN') == 1)

# Variant B: KDE shape (valley = -1 or -2)
valley_ticks = df.filter(pl.col('ValleysPeaks') <= -1)

# Combined: valley + imbalance + big bubble
signal_ticks = df.filter(
    (pl.col('ValleysPeaks') <= -1) &
    (pl.col('market_state').str.contains('imbalance')) &
    (pl.col('Volume') >= 10) &
    (pl.col('SessionType') == 'RTH')
)
print(f"Found {signal_ticks.shape[0]} signal-qualified ticks")
```

### Joining External Data (VIX, GEX)

```python
import pandas as pd

# Load trade results
trades_df = pd.read_parquet('sources/ES/parquet/trades_A_LVN_202501.parquet')

# Load VIX regime (daily, prior day values)
vix = pd.read_csv('sources/VIX/vix_data.csv', parse_dates=['date'])
vix = vix[['date', 'is_contango_prev', 'contango_f2_minus_f1_prev']]

# Load GEX (daily, prior day values)
gex = pd.read_csv('sources/GEX/dix.csv', parse_dates=['date'])
gex = gex[['date', 'is_long_gamma_prev', 'gex_prev']]

# Merge by date
trades_df['date'] = trades_df['entry_datetime'].dt.date
trades_df = trades_df.merge(vix, left_on='date', right_on='date')
trades_df = trades_df.merge(gex, left_on='date', right_on='date')

# Breakdown by VIX regime
contango_trades = trades_df[trades_df['is_contango_prev'] == True]
backwardation_trades = trades_df[trades_df['is_contango_prev'] == False]

print(f"Contango: PF={contango_trades['pnl'].sum() / abs(contango_trades[contango_trades['pnl']<0]['pnl'].sum()):.2f}")
print(f"Backwardation: PF={backwardation_trades['pnl'].sum() / abs(backwardation_trades[backwardation_trades['pnl']<0]['pnl'].sum()):.2f}")
```

---

## Backtester API Reference

### BacktestEngine

```python
from orderflow.backtester import BacktestEngine, FixedTPSLExit

engine = BacktestEngine(
    tick_size=0.25,      # ES minimum price increment
    tick_value=12.50,    # Dollar value per tick ($0.25 * $50 per tick = $12.50)
    slippage_model=SlippageModel.RANDOM,  # None | FIXED | RANDOM | IMPACT
    slippage_params={'pct': 0.0001},      # 0.01% of entry price
)

result = engine.run(
    ticks=df,                              # Full enriched tick DataFrame
    signals=signals_df,                    # Columns: Index, signal_direction, entry_price, stop_loss, tp_price, ...
    exit_strategy=FixedTPSLExit(tp=50, sl=8),  # Exit rule
    risk_manager=RiskManager(daily_max_loss=-2000, max_trades_per_day=10),  # Optional
    use_numba=True,                        # Use JIT acceleration (default True)
)

print(result.metrics.summary())
print(result.trades)  # DataFrame of closed trades
```

### Exit Strategies

```python
from orderflow.backtester import (
    FixedTPSLExit,        # Fixed stop/target in ticks
    TrailingStopExit,     # Trailing stop (ticks behind favorable price)
    TimeBasedExit,        # Exit after N bars
    VolatilityExit,       # Exit when volatility drops below threshold
    DynamicExit,          # Adaptive stops based on ATR
    CVDBreakEvenExit,     # Mechanical break-even using CVD
    CompositeExit,        # Combine multiple strategies (OR logic)
)

# Fixed stop/target
exit1 = FixedTPSLExit(tp=50, sl=8)

# Trailing stop (doesn't use fixed TP)
exit2 = TrailingStopExit(trailing_ticks=15, initial_sl=8)

# Time-based: hold 30 bars max
exit3 = TimeBasedExit(max_bars=30)

# Combine: exit if any condition hit
exit_combo = CompositeExit([
    FixedTPSLExit(tp=50, sl=8),
    TimeBasedExit(max_bars=30),
])
```

### PerformanceMetrics

```python
from orderflow.backtester import compute_metrics

metrics = result.metrics
print(metrics.summary())  # Pretty-printed stats

# Individual metrics
print(metrics.win_rate)           # 52.3%
print(metrics.profit_factor)      # 2.8
print(metrics.max_drawdown)       # -$4,200
print(metrics.sharpe_ratio)       # 1.23
print(metrics.sortino_ratio)      # 1.56
print(metrics.calmar_ratio)       # 0.98
print(metrics.expectancy)         # $15.50 per trade
print(metrics.recovery_factor)    # (total_pnl / max_drawdown)

# Trade-level analysis
mae_df = metrics.compute_mae_mfe_analysis(result.trades)
# Breakdown: winners' MAE, losers' MAE/MFE, ratio of winners with >5 tick MAE, etc.
```

---

## Developing New Strategies

### Step 1: Document the Strategy

Create `docs/strategies/<strategy_name>.md` with:
- Source (paper, video, trader, etc.)
- Core concept (3–5 sentences)
- Rules (entry, exit, filters, session constraints)
- Backtest results (Jan 2025 – Feb 2026 minimum, IS/OOS split preferred)
- Known issues and open questions

Example: `docs/strategies/valentini_mean_reversion_poc.md`

### Step 2: Build a Backtest Runner

Create `orderflow/runners/runner_data_<strategy>_backtest.py`:

```python
import polars as pl
from orderflow.backtester import BacktestEngine, FixedTPSLExit

def generate_signals(df, variant='default', **filters):
    """
    Generate entry signals from enriched tick DataFrame.
    
    Args:
        df: Enriched tick DataFrame (output from runner_data_enrichment)
        variant: Strategy variant name (e.g., 'A_LVN', 'B_valley')
        **filters: Additional filter kwargs (e.g., big_order_threshold=10)
    
    Returns:
        signals: DataFrame with columns [Index, signal_direction, entry_price, stop_loss, tp_price, ...]
    """
    df = df.filter(
        (pl.col('SessionType') == 'RTH') &
        (pl.col('Prev_POC') > 0) &
        (pl.col('market_state').str.contains('imbalance'))
    )
    
    # Variant-specific filtering
    if variant == 'A_LVN':
        df = df.filter(pl.col('LVN') == 1)
    elif variant == 'B_valley':
        df = df.filter(pl.col('ValleysPeaks') <= -1)
    
    # Group by bar and find first big bubble per bar
    signals = []
    for bar_idx, bar_group in df.group_by('current_bar_datetime'):
        big_orders = bar_group.filter(pl.col('Volume') >= filters.get('big_order_threshold', 10))
        if big_orders.shape[0] > 0:
            trigger_tick = big_orders[0]  # First big bubble
            signals.append({
                'Index': trigger_tick['Index'],
                'signal_direction': 'long' if trigger_tick['TradeType'] == 2 else 'short',
                'entry_price': trigger_tick['next_bar_open'],
                'stop_loss': trigger_tick['AskPrice'] - 2 * 0.25,  # 2 ticks
                'tp_price': trigger_tick['Prev_POC'],
            })
    
    return pl.DataFrame(signals)

def run_backtest(df, month='202501', variant='A_LVN'):
    signals = generate_signals(df, variant=variant, big_order_threshold=10)
    signals = signals.sort('Index')  # CRITICAL: sort by Index
    
    engine = BacktestEngine(tick_size=0.25, tick_value=12.50)
    result = engine.run(df, signals, exit_strategy=FixedTPSLExit(tp=50, sl=8))
    
    # Save results
    result.trades.write_parquet(f'sources/ES/parquet/trades_{variant}_{month}.parquet')
    return result

if __name__ == '__main__':
    df = pl.read_parquet('sources/ES/parquet/202501_ES.parquet')
    result = run_backtest(df, month='202501', variant='A_LVN')
    print(result.metrics.summary())
```

### Step 3: Build an Analysis Runner

Create `orderflow/runners/runner_trades_<strategy>_analysis.py`:

```python
import polars as pl
import pandas as pd
from glob import glob

def analyze_strategy(variant='A_LVN', start_month='202501', end_month='202512'):
    """Concatenate all monthly trade files and compute full-period stats."""
    
    # Load and concatenate trades
    trade_files = glob(f'sources/ES/parquet/trades_{variant}_*.parquet')
    trades = pd.concat([pd.read_parquet(f) for f in trade_files])
    
    # Recalculate trade_id (monthly files start from 1)
    trades['trade_id'] = range(1, len(trades) + 1)
    
    # Load external data
    vix = pd.read_csv('sources/VIX/vix.csv', parse_dates=['date'])
    gex = pd.read_csv('sources/GEX/dix.csv', parse_dates=['date'])
    
    # Merge
    trades['date'] = trades['entry_datetime'].dt.date
    trades = trades.merge(vix[['date', 'is_contango_prev']], on='date')
    trades = trades.merge(gex[['date', 'is_long_gamma_prev']], on='date')
    
    # Compute metrics
    total_pnl = trades['pnl'].sum()
    win_rate = (trades['pnl'] > 0).sum() / len(trades)
    profit_factor = trades[trades['pnl'] > 0]['pnl'].sum() / abs(trades[trades['pnl'] < 0]['pnl'].sum())
    
    print(f"\n{variant} — Full Period ({start_month} to {end_month})")
    print(f"Trades: {len(trades)}")
    print(f"Win Rate: {win_rate:.1%}")
    print(f"Profit Factor: {profit_factor:.2f}")
    print(f"Total P&L: ${total_pnl:,.0f}")
    
    # Breakdown by hour
    by_hour = trades.groupby(trades['entry_datetime'].dt.hour).agg({'pnl': 'sum'})
    print(f"\nBy Hour:\n{by_hour}")
    
    # Breakdown by VIX regime
    contango = trades[trades['is_contango_prev'] == True]['pnl'].sum()
    backwardation = trades[trades['is_contango_prev'] == False]['pnl'].sum()
    print(f"\nVIX Regime:")
    print(f"  Contango: ${contango:,.0f}")
    print(f"  Backwardation: ${backwardation:,.0f}")

if __name__ == '__main__':
    analyze_strategy(variant='A_LVN')
    analyze_strategy(variant='B_valley')
```

### Step 4: Validate Results

Run the backtest on Jan 2025 – Sep 2025 (in-sample), validate on Oct 2025 – Dec 2025 (out-of-sample).

For approval, show:
- Profit Factor >= 1.5 (both IS and OOS)
- Win rate >= 45%
- Reasonable max drawdown (< 15% of max profit)
- No curve-fitting to individual months (good across all months, not just lucky ones)

---

## Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest orderflow/test/test_volume_profile.py -v

# Run with coverage
pytest --cov=orderflow --cov-report=html

# Run backtester tests only
pytest orderflow/test/ -k backtester -v
```

---

## Troubleshooting

### Kernel Crash During Backtest (Windows 32GB)

**Cause:** Out of memory. Each monthly file (~1.2M ticks) + intermediate DataFrames fills most of 32GB.

**Fix:** Process one month at a time. After each month, explicitly delete large DataFrames and call `gc.collect()`:

```python
for month in ['202501', '202502', ...]:
    df = pl.read_parquet(f'sources/ES/parquet/{month}_ES.parquet')
    result = engine.run(df, signals)
    
    # CRITICAL: clean up
    del df, result, signals
    import gc
    gc.collect()
```

### Signal Pointer Desync

**Cause:** Signals not sorted by `Index`. Backtest matches signals sequentially; unsorted signals get matched to wrong ticks.

**Fix:** Always sort before passing to engine:

```python
signals = signals.sort('Index')  # or .sort_values('Index') for Pandas
result = engine.run(df, signals)
```

### Prev_POC Is Zero (First Session)

**Cause:** First session of dataset has no prior session, so Prev_POC = 0. This produces NaN in TP target.

**Fix:** Filter during signal generation:

```python
df = df.filter(pl.col('Prev_POC') > 0)
```

### VIX/GEX Lookahead Bias

**Cause:** Merging today's trade file with today's VIX closes. But VIX is end-of-day (4pm ET), trades happened intraday.

**Fix:** Always use `.shift(1)` when creating external data columns:

```python
vix['is_contango_prev'] = vix['is_contango'].shift(1)  # Day T uses day T-1 close
```

---

## Development

### Clone and Install

```bash
git clone https://github.com/andreaferrante/orderflow.git
cd orderflow
pip install -e ".[dev]"
```

### Pre-commit Hooks

Enforced on every commit:
- Conventional commit format: `type(scope): description`
- Type: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`
- Scope (optional): `backtester`, `volume_profile`, `stats`, etc.

Example: `feat(backtester): add CVD break-even exit strategy`

### Code Style

- Follow Black formatting (line length 88)
- Type hints required for public functions
- Docstrings for modules and classes (Google style)
- No commented-out code

### Contributing

1. Create a feature branch: `git checkout -b feat/your-feature`
2. Write tests for new functionality
3. Ensure all tests pass: `pytest`
4. Commit with conventional message: `git commit -m "feat(module): description"`
5. Push and create a pull request

---

## Known Limitations & Future Work

### Current Limitations

1. **Order Flow Quality** — Raw tick data may lose ticks during high-velocity periods (CPI, FOMC, gaps). Estimated error ~2% of sessions. Backtest results contain this measurement error.

2. **Scale Limitations** — 32GB RAM machine processes one month at a time; can't load full-year dataset in memory. Workaround: per-month processing + concatenation.

3. **DOM Depth** — Data currently includes L1 only (bid/ask); deeper DOM not available. Advanced microstructure analysis (order book imbalance) not supported.

4. **Single Instrument** — Currently ES-only. Infrastructure supports any tick data with TradeType; multi-symbol support (NQ, /YM) is planned.

5. **Session-Aware Exit** — All positions force-closed at session end (15:00 CT). No overnight holding. This is intentional (Valentini rule), but limits certain strategies.

### Future Enhancements

- [ ] NQ (Nasdaq) and /YM (Russell) data support
- [ ] Deeper DOM (L2-L10) imbalance detection
- [ ] GPU-accelerated backtester (CuPy)
- [ ] Web UI for live backtesting
- [ ] Multi-timeframe analysis (daily bar correlation with tick trades)
- [ ] Regime-conditioned portfolio optimization
- [ ] Machine learning regime classifier (replacing HMM)

---

## License

MIT License. See `LICENCE` file.

---

## Citation

If you use Orderflow in research or development, please cite:

```bibtex
@software{orderflow2025,
  title={Orderflow: Institutional-Grade Tick Data Analysis and Backtesting},
  author={Ferrante, Andrea},
  year={2025},
  url={https://github.com/andreaferrante/orderflow},
  version={0.5.0}
}
```

---

## Support

- **Issues:** [GitHub Issues](https://github.com/andreaferrante/orderflow/issues)
- **Strategy Docs:** See `docs/strategies/` for detailed rule specifications
- **API Docs:** In-code docstrings and module READMEs

---

## Acknowledgments

Strategy rules extracted from:
- **Valentini Trend Following** — Fabio Valentini, "Chart Fanatics" podcast
- **Market Microstructure** — James Dalton, "Markets in Profile"
- **Auction Theory** — Peter Steidlmayer, auction-market-theory framework

Special thanks to the Polars and Numba communities for performance-critical libraries.
