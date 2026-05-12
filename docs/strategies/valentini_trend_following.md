# Valentini Trend Following Strategy

## Source
Video: "Chart Fanatics" podcast with Fabio Valentino (Valentini).
Rules extracted directly from the video transcript. All quotes are from Fabio unless noted.

---

## Core Concept

A trend following model for NY RTH session on equity index futures (ES, NQ).
The model does NOT predict direction — it reads market state and enters only when
the market itself confirms the move through aggression (big orders).

*"Your ability to predict is zero but your ability to read is 100."*

Three-step framework:
1. **Market State** — only trade imbalance, never balance
2. **Location** — find the LVN where price returns after breakout
3. **Trigger** — big bubble (aggressive order) confirms entry

---

## Rules

### Session Filter
- **RTH only**, 08:30–15:00 CT
- No overnight positions — close everything at session end
- *"I don't advise to keep the trades for the night"*
- Do not trade during the first 15–20 minutes of session open (noise)
- `Prev_POC` must be > 0 (skip first session in dataset)

### Step 1 — Market State
- Price **outside** Value Area (`VA_Areas == "na"`) → imbalance → model active
- Price **inside** Value Area (`VA_Areas in ["VA", "PO"]`) → balance → skip
- *"We can only have two market state: balanced and imbalanced. With this model we want to transact in imbalance."*

Classification:
- `imbalance_up`: outside VA, Price > POC, Prev_POC > Price → LONG context
- `imbalance_down`: outside VA, Price < POC, Prev_POC < Price → SHORT context
- `imbalance_no_target`: outside VA but Prev_POC not aligned → skip

### Step 2 — Location
Price must be at a **Low Volume Node (LVN)** in the volume profile.
*"You search for low volume node. The probability that we will go down is really high."*

Two variants tested:
- **A_LVN**: `LVN == 1`
- **B_valley**: `ValleysPeaks <= -1`

A_LVN is more selective and has shown better backtest results.

### Step 3 — Trigger
- **LONG**: `TradeType == 2` (Ask Trade = BUY aggression) + `Volume >= threshold`
- **SHORT**: `TradeType == 1` (Bid Trade = SELL aggression) + `Volume >= threshold`
- Threshold: 10 contracts on ES produces best results. 50 better for A_LVN only.
- Only the **first** qualifying bubble per 1-minute bar is valid

### Opposing Bubble Filter
After the trigger, if any opposing big bubble appears in the same bar → invalidate.
Same threshold applies. Logic: `opposing_type = 3 - TradeType` (1↔2).

### Bar Close Confirmation
- **LONG**: `close > AskPrice AND close > open`
- **SHORT**: `close < BidPrice AND close < open`
- *"I need a full body candle close above this level"*

### Entry
- `entry_price = next_bar_open`
- Entry bar must open within the session window (filter on `next_bar_datetime`, not trigger time)
- Prev_POC alignment: LONG → `entry_price < Prev_POC`, SHORT → `entry_price > Prev_POC`

### Stop Loss
- Based on trigger tick price, not entry price
- LONG: `AskPrice - STOP_TICKS * TICK_SIZE`
- SHORT: `BidPrice + STOP_TICKS * TICK_SIZE`
- *"Put your stop loss one or two ticks below the high — taken out before acceleration"*
- Fabio visually places stop tangent to the bubble circle (proportional to size) — not yet mechanically implemented

### Take Profit
- Target = `Prev_POC` — full exit, no partials
- *"The probability that the market will reverse from it 70% of the time"*

### CVD Break-Even
- Save `cvd_at_entry` at trigger tick
- Activate break-even when both are true simultaneously:
  - MFE >= `min_profit_ticks` (default 2)
  - LONG: `current_CVD > cvd_at_entry` / SHORT: `current_CVD < cvd_at_entry`
- Activates once, never reverts

### Forced Exit
All positions closed at 15:00 CT.

---

## Backtest Results (Jan 2025 – Feb 2026)

### Without filters

| Variant | Trades | Win Rate | Net P&L | PF | Max DD |
|---------|--------|----------|---------|-----|--------|
| A_LVN th=10 | 75 | 13.3% | -$1,843 | 0.56 | $2,358 |
| B_valley th=10 | 150 | 10.7% | -$5,060 | 0.45 | $6,055 |
| A_LVN th=50 | 52 | 15.4% | -$1,484 | 0.70 | $2,242 |

### Best filter combinations

**A_LVN th=10:**

| Filter | Trades | Win Rate | P&L | PF |
|--------|--------|----------|-----|----|
| ore=[9,14] | 23 | 30.4% | +$804 | 1.71 |
| ore=[9] + backwardation | 14 | 35.7% | +$387 | 1.69 |
| ore=[14] | 8 | 25.0% | +$630 | 2.77 |

VIX and GEX do not add value for A_LVN when hour filter is applied.
Hour filter alone (ore=[9,14]) is the most robust — no external data needed.

**B_valley th=10:**

| Filter | Trades | Win Rate | P&L | PF |
|--------|--------|----------|-----|----|
| ore=[11,12] + gex_short | 23 | 13.0% | +$1,992 | 2.44 |

GEX short gamma strongly beneficial for B_valley at hours 11-12.
VIX not useful — all B_valley trades fall in backwardation regardless.

---

## Structural Observations

- **Loser MAE mediana 4-5 ticks** — losing trades go against immediately. Wrong signal at the root.
- **Winner MFE medio 19-35 ticks** — winners move fast with minimal adverse excursion.
- **Hours 10, 11, 13 systematically negative** for trend following.
- **Hour 9** (NY open momentum) and **Hour 14** (last session impulse) are productive for A_LVN.
- **prev_session_range**: winners have LOWER range than losers — moderate range days produce cleaner trends.
- **Short gamma regime**: amplifies directional moves, favorable for trend following.

---

## Known Issues & Fixes

### signal_ptr desync (FIXED in engine.py)
When a trade closes via `time_exit`, the signal pointer must skip all signals
that occurred during the trade. Fix in `_run_python`:
```python
while signal_ptr < len(signal_sides) and signal_ts[signal_ptr] <= timestamps[i]:
    signal_ptr += 1
```
Without this fix, all trades after any `time_exit` use the wrong TradeType.

### Signal DataFrame must be sorted by Index
`df_signals_pd.sort_values("Index").reset_index(drop=True)` before passing to engine.

### Entry bar session boundary
Filter on `next_bar_datetime` not `current_bar_datetime`.
Trigger at 14:59 with entry at 15:00 must be discarded. Applied in Step B.

---

## Not Yet Implemented

- Stop loss proportional to bubble size (tangent to circle)
- "No obstacles" filter: path to Prev_POC free of HVN
- Directional efficiency filter as alternative to prev_session_range
- CVD break-even with calibrated min_profit_ticks

---

## Environment Setup

### File Paths (Windows)
```
Tick data (enriched parquet):  C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/ES/parquet/
Trade results output:          C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/ES/parquet/
GEX/DIX data:                  C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/GEX/dix.csv
VIX data:                      (use vix_utils library, pre-computed CSV in sources folder)
```

### Ticker Configuration
```python
import orderflow.configuration as cf
TICKER     = "ES"
tick_size  = cf.FUTURE_VALUES.loc[cf.FUTURE_VALUES['Ticker'] == TICKER, 'Tick_Size'].values[0]   # 0.25
tick_value = cf.FUTURE_VALUES.loc[cf.FUTURE_VALUES['Ticker'] == TICKER, 'Tick_Value'].values[0]  # 12.50
```

### Backtest Engine Setup
```python
from orderflow.backtester.engine import BacktestEngine
from orderflow.backtester.execution import SlippageMode, SlippageModel

engine = BacktestEngine(
    tick_size=tick_size,
    tick_value=tick_value,
    commission=0.9,
    n_contracts=1,
    slippage_model=SlippageModel(mode=SlippageMode.UNIFORM, max_ticks=2, seed=42),
    progress_bar=False,
)
```

### Available Monthly Files
Pattern: `YYYYMM_ES.parquet` — Jan 2025 through Feb 2026.
Known gaps: April 2025 missing, December 2025 partial.

### Columns Needed from Parquet
Refer to `CLAUDE.md` for the full `COLUMNS_NEEDED` list. At minimum include:
`Index`, `Sequence`, `Date`, `Time`, `Datetime`, `Hour`, `SessionType`,
`Prev_POC`, `TradeType`, `Volume`, `AskPrice`, `BidPrice`, `Price`,
`VA_Areas`, `POC`, `Session_High`, `Session_Low`, `LVN`, `ValleysPeaks`,
`CD_Ask`, `CD_Bid`, `current_bar_askvolume`, `current_bar_bidvolume`,
`Node_Ask_Volume`, `Node_Bid_Volume`,
`current_bar_datetime`, `current_bar_open`, `current_bar_high`, `current_bar_low`, `current_bar_close`,
`next_bar_datetime`, `next_bar_open`, `next_bar_high`, `next_bar_low`, `next_bar_close`

### Data for Backtest Engine
The engine requires a tick DataFrame with these columns:
`Date`, `Datetime`, `Index`, `Price`, `SessionType`, `Time`
Plus any indicator columns passed via `indicator_columns` parameter (e.g. `CVD`).
