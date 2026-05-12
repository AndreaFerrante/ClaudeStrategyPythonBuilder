# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

---

## Project Overview

**Orderflow** is a Python package for institutional-grade tick-by-tick trading data analysis and orderflow research. It provides reusable modules for data compression, backtesting, statistical analysis, and market microstructure indicators.

The package uses **Polars** for large dataset operations, **Pandas** for analysis and backtest results, and **Numba** for performance-critical paths.

---

## Project Structure

### Reusable Modules (`orderflow/`)

```
orderflow/
├── backtester/         # Tick-by-tick backtest engine
│   ├── engine.py       # BacktestEngine, BacktestResult
│   ├── exits.py        # Exit strategies (FixedTPSL, Trailing, Dynamic, CVDBreakEven, etc.)
│   ├── execution.py    # SlippageModel, FillSimulator
│   ├── risk.py         # RiskManager (mechanical TP/SL)
│   └── metrics.py      # PerformanceMetrics, post-trade analytics
│
├── compressor/         # Tick aggregation into bars
│   └── compressor.py   # Volume, range, time bars
│
├── stats/              # Statistical analysis & regime detection
│   ├── stats.py        # Risk metrics (Sharpe, Sortino, Calmar, etc.)
│   ├── hypothesis.py   # ADF, KPSS, Jarque-Bera, CUSUM
│   ├── montecarlo.py   # Bootstrap with confidence intervals
│   └── markov.py       # Markov chains, HMM regime detection
│
├── volume_profile.py   # POC, VAH/VAL, dynamic CVD
├── auctions.py         # Auction theory, block aggregation
├── compressor.py       # Tick-to-bar compression
└── _volume_factory.py  # Volume distribution utilities
```

### Runner Files

The actual research workflow is implemented in standalone runner scripts, not inside the package. There are three types:

---

#### `runner_data_enrichment.py`
Runs once when new monthly tick data becomes available. Loads raw tick-by-tick data, computes all indicators (volume profile, CVD, LVN, ValleysPeaks, 1-minute bars, session fields, etc.) and saves the enriched output as a monthly Parquet file. The output file is the input for all subsequent strategy runners. There is no need to rerun this file unless source tick data changes or new indicators are added.

---

#### `runner_data_<strategyname>_backtest.py`
One file per strategy. Loads the monthly enriched Parquet files produced by the data enrichment runner and executes:

1. **Signal generation** (Step B equivalent) — filters ticks by market state, location flags, big bubble threshold, bar confirmation, and other strategy-specific conditions
2. **Backtest execution** — runs the backtest engine with the appropriate exit strategy
3. **Trade output** — saves one Parquet file per month containing the trade results

Due to memory constraints on 32GB Windows machines, the backtest must be run one month at a time. After each month, all large DataFrames must be explicitly deleted and `gc.collect()` called before loading the next month. Failure to do this causes kernel crashes.

Trade output files follow the naming convention `trades_<variant>_<YYYYMM>.parquet`. Use glob patterns specific enough to avoid mixing variants when loading.

---

#### `runner_trades_<strategyname>_analysis.py`
One file per strategy. Loads and concatenates all monthly trade Parquet files, joins external daily data (VIX, GEX), and runs the full analysis suite:

- Overall statistics (win rate, P&L, profit factor, max drawdown)
- MAE/MFE distribution for winners and losers
- Breakdown by entry hour
- Breakdown by VIX regime (contango / backwardation)
- Breakdown by GEX regime (long gamma / short gamma)
- Combined regime filtering to identify profitable subsets

After concatenation, always recalculate `trade_id` as a sequential integer since each monthly file starts from 1.

---

## Installation & Development

```bash
# Install in editable mode (required — do not edit site-packages directly)
pip install -e .

# Run tests
pytest
pytest orderflow/test/test_volume_bars.py -v
pytest --cov=orderflow
```

---

## TradeType Convention (Sierra Chart / ES Futures)

**Critical and counterintuitive.** Always use this convention:

| TradeType | Sierra Chart Label | Meaning | Direction |
|-----------|-------------------|---------|-----------|
| `1` | Bid Trade | SELL aggression — executed on the bid | SHORT trigger |
| `2` | Ask Trade | BUY aggression — executed on the ask | LONG trigger |

Backtest engine convention (`orderflow/backtester/engine.py`):
```python
side = Side.LONG if side_code == 2 else Side.SHORT
```

CVD delta convention:
```python
tick_delta = +Volume  if TradeType == 2  # buy on ask → positive
tick_delta = -Volume  if TradeType == 1  # sell on bid → negative
CVD = CD_Bid - CD_Ask                   # cumulative from ETH session start
```

---

## Enriched Tick DataFrame Columns

Produced by `runner_data_enrichment.py` and stored as `YYYYMM_ES.parquet`.

### Identification & Ordering
| Column | Type | Description |
|--------|------|-------------|
| `Index` | int64 | Unique sequential tick identifier. Used by the backtest engine to match signals. |
| `Sequence` | int64 | Ordering within a 1-minute bar. Used to identify the first bubble per bar. |
| `Date` | date | Trading date (CT). |
| `Time` | time | Tick time (CT). |
| `Datetime` | datetime | Full timestamp, microsecond precision, no timezone info. |
| `Hour` | int8 | Hour extracted from Datetime. Used for session filtering. |

### Session
| Column | Type | Description |
|--------|------|-------------|
| `SessionType` | str | `"RTH"` = 08:30–16:00 CT, `"ETH"` = Extended Hours. Volume profile resets at ETH open. |
| `Session_High` | float64 | Rolling high from ETH open to current tick. |
| `Session_Low` | float64 | Rolling low from ETH open to current tick. |

### Price & Volume
| Column | Type | Description |
|--------|------|-------------|
| `Price` | float64 | Last trade price. |
| `Volume` | int64 | Contracts traded at this tick. |
| `TradeType` | int64 | `1`=Bid/SELL, `2`=Ask/BUY. See convention above. |
| `AskPrice` | float64 | Best ask at tick time. Trigger price for LONG signals. |
| `BidPrice` | float64 | Best bid at tick time. Trigger price for SHORT signals. |

### Volume Profile (Rolling, resets each ETH session)
| Column | Type | Description |
|--------|------|-------------|
| `POC` | float64 | Point of Control — highest volume price in the current ETH session. |
| `Prev_POC` | float64 | POC of the previous ETH session at close. Used as TP target. Zero for the first session — always filter with `Prev_POC > 0`. |
| `VA_Areas` | str | `"VA"` = inside Value Area, `"PO"` = on POC, `"na"` = outside (imbalance). |
| `LVN` | int8 | `1` if price is at a Low Volume Node, `0` otherwise. Location flag variant A. |
| `ValleysPeaks` | float64 | KDE shape: `2`=peak extreme, `1`=peak area, `0`=undefined, `-1`=valley area, `-2`=valley extreme. Location flag variant B (`<= -1`). |
| `Node_Ask_Volume` | float64 | Cumulative ask volume at this price level in the rolling VP. |
| `Node_Bid_Volume` | float64 | Cumulative bid volume at this price level in the rolling VP. |

### Cumulative Delta
| Column | Type | Description |
|--------|------|-------------|
| `CD_Ask` | float64 | Cumulative ask volume from ETH session start. |
| `CD_Bid` | float64 | Cumulative bid volume from ETH session start. |
| `CVD` | float64 | `CD_Bid - CD_Ask`. Positive = buy pressure dominant. Resets each ETH session. |
| `current_bar_askvolume` | float64 | Ask volume in the current 1-minute bar. |
| `current_bar_bidvolume` | float64 | Bid volume in the current 1-minute bar. |
| `current_bar_cvd` | float64 | `current_bar_bidvolume - current_bar_askvolume`. Delta for current bar only. |
| `node_cvd` | float64 | `Node_Bid_Volume - Node_Ask_Volume`. Historical delta at this VP level. |
| `tick_delta` | float64 | Signed tick volume: +Volume for BUY, -Volume for SELL. |

### 1-Minute Bars — Current Bar
| Column | Type | Description |
|--------|------|-------------|
| `current_bar_datetime` | datetime | Open timestamp of the bar containing this tick. Grouping key for signal generation. |
| `current_bar_open` | float64 | Bar open price. |
| `current_bar_high` | float64 | Bar high price. |
| `current_bar_low` | float64 | Bar low price. |
| `current_bar_close` | float64 | Bar close price. Used for bar confirmation logic. |

### 1-Minute Bars — Next Bar
| Column | Type | Description |
|--------|------|-------------|
| `next_bar_datetime` | datetime | Open timestamp of the next 1-minute bar. |
| `next_bar_open` | float64 | Entry price for signals triggered in the current bar. |
| `next_bar_high` | float64 | Next bar high. |
| `next_bar_low` | float64 | Next bar low. |
| `next_bar_close` | float64 | Next bar close. |

### Derived Columns (computed in enrichment runner)
| Column | Type | Description |
|--------|------|-------------|
| `market_state` | str | `"balance"` = inside VA, `"imbalance_up"` = outside VA above POC with Prev_POC above price, `"imbalance_down"` = outside VA below POC with Prev_POC below price, `"imbalance_no_target"` = outside VA but Prev_POC not aligned. |
| `big_ask` | bool | TradeType==2 AND Volume >= threshold. BUY aggression flag. |
| `big_bid` | bool | TradeType==1 AND Volume >= threshold. SELL aggression flag. |
| `prev_session_range` | float64 | `Session_High - Session_Low` of the previous ETH session. Proxy for previous day directional quality. Must be computed before the RTH session filter. |

---

## Signal DataFrame Columns

Produced by strategy-specific signal generation logic inside the backtest runner.

| Column | Description |
|--------|-------------|
| `signal_index` | Index of the trigger tick (the big bubble). |
| `Index` | Index of the first tick of the entry bar. Used by the backtest engine. **Must be sorted ascending before passing to engine.** |
| `signal_direction` | `"long"` or `"short"`. |
| `entry_price` | `next_bar_open` — fill price before slippage. |
| `stop_loss` | LONG: `AskPrice - STOP_TICKS * TICK_SIZE` / SHORT: `BidPrice + STOP_TICKS * TICK_SIZE`. Based on trigger price, not entry price. |
| `Prev_POC` | Take profit target. Full exit at this level. |
| `TP_Ticks` | `abs(Prev_POC - entry_price) / tick_size`. Computed before passing to engine. |
| `SL_Ticks` | `abs(stop_loss - entry_price) / tick_size`. Computed before passing to engine. |
| `variant` | Strategy variant label (e.g. `"A_LVN"`, `"B_valley"`). |

---

## Data Storage

### Enriched Tick Data
```
sources/ES/parquet/
    202501_ES.parquet
    202502_ES.parquet
    ...
```
One file per month. Contains all enriched tick columns. Known gaps: April 2025 missing, December 2025 partial.

### Trade Results
```
sources/ES/parquet/
    trades_A_LVN_202501.parquet
    trades_B_valley_202501.parquet
    ...
```
One file per strategy variant per month. Concatenated for full-period analysis.

### External Data
```
sources/GEX/dix.csv       ← GEX and DIX (SqueezeMetrics)
sources/VIX/<vix_file>    ← VIX term structure (vix_utils library)
```

---

## External Data Sources

### VIX Term Structure
- **Source:** `vix_utils` Python library (CBOE data), daily granularity

| Column | Description |
|--------|-------------|
| `date` | Trading date |
| `is_contango_prev` | Boolean — True if term structure was in contango (F2 > F1) the previous day. Primary filter column. |
| `regime_prev` | `"contango"` or `"backwardation"` |
| `contango_f2_minus_f1_prev` | F2 minus F1 spread. Positive = contango, negative = backwardation. |

For trend following, trade only when `is_contango_prev == False`. Contango signals market stability expectations — directional breakouts are less sustained.

### GEX / DIX (SqueezeMetrics)
- **Source:** `dix.csv` from SqueezeMetrics website, daily end-of-day

| Column | Description |
|--------|-------------|
| `date` | Trading date |
| `gex` | Gamma Exposure. Positive = long gamma (dampens moves). Negative = short gamma (amplifies moves). |
| `dix` | Dark Index — proxy for institutional dark pool buying pressure |
| `gex_ma252` | 252-day rolling mean of GEX. Baseline for regime classification. |
| `is_long_gamma_prev` | Boolean — True if `gex > gex_ma252` the previous day. Primary filter column. |
| `gex_prev` | Raw GEX value of the previous day. Use for quartile analysis. |
| `dix_prev` | DIX value of the previous day. |

For trend following, prefer `is_long_gamma_prev == False` (short gamma). Short gamma causes market maker hedging to amplify directional moves.

**Sign convention:** negative GEX = short gamma = trend amplifier. Positive GEX = long gamma = trend dampener.

**Lookahead bias:** both VIX and GEX are end-of-day values. Always apply `.shift(1)` when constructing these DataFrames so that day T uses values from day T-1.

---

## Known Issues & Critical Notes

### signal_ptr desync bug (FIXED)
In `orderflow/backtester/engine.py`, `_run_python`: when a position closes for any reason (including `time_exit`), the signal pointer must skip all signals that occurred during the trade. Without this, trades following a `time_exit` receive the wrong TradeType and execute in the wrong direction. Fix:
```python
while signal_ptr < len(signal_sides) and signal_ts[signal_ptr] <= timestamps[i]:
    signal_ptr += 1
```

### Signal DataFrame must be sorted by Index
The backtest engine consumes signals sequentially by position in the array, not by timestamp lookup. If the signal DataFrame is not sorted by `Index` ascending, signals will be matched to the wrong ticks.

### Tick data quality
Real-time tick collection may lose ticks during high-velocity periods (CPI, FOMC, gap openings). This can shift end-of-session POC by a few ticks, affecting `Prev_POC` the following day. Estimated error rate < 2% of sessions. Backtest results should be treated as containing this measurement error.

### BIG_ORDER_THRESHOLD
`10` contracts = ~0.38% of RTH ticks for ES. Threshold `50` performs better for variant A_LVN. Threshold `20` is consistently worse than `10` across all variants.

---

## Design Patterns

- **Dual DataFrame support**: functions support both Pandas and Polars where possible. Polars preferred for large datasets.
- **Numba acceleration**: hot-path loops in the backtester use Numba JIT with pure Python fallbacks.
- **No lookahead bias**: all rolling operations are strictly causal. Rolling windows use only past data.
- **Editable install**: always use `pip install -e .`. Never edit files in site-packages directly.
- **Conventional commits**: pre-commit hook enforces `type(scope): description` format (commitizen).

## Strategy Knowledge Base
Before implementing or modifying any strategy runner, read the relevant file in `docs/strategies/`.
- Index: `docs/strategies/README.md`
- Valentini Trend Following: `docs/strategies/valentini_trend_following.md`
- Valentini Mean Reversion: `docs/strategies/valentini_mean_reversion.md`

## Agent Definitions
When working with multiple agents, role definitions are in `docs/agents/`.
- Planner: `docs/agents/planner.md` — model: claude-opus-4-5
- Coder: `docs/agents/coder.md` — model: claude-sonnet-4-5  
- Reviewer: `docs/agents/reviewer.md` — model: claude-opus-4-5
