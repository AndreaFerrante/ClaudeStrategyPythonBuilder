# Valentini Mean Reversion Strategy

## Source
Video: "Chart Fanatics" podcast with Fabio Valentino (Valentini).
Rules extracted from the video transcript combined with empirical discovery
during backtest development (see Discovery section below).

---

## Core Concept

A mean reversion model that works in **balance** (consolidation) market conditions.
Complementary to the trend following model — while trend following requires imbalance,
mean reversion works precisely when the market is compressed and ranging.

*"One other model that I am using is the one that takes opportunity of when the market
is deep discount based on the volume distribution and snaps back in."*

Fabio uses this model primarily during:
- **London session**
- **Summer months** (May–August) when markets are compressed
- As a complement to trend following — when one loses the other profits

---

## Rules (from video)

### Session Filter
- Primary: **London session**
- Also works in NY session during compression periods (summer)
- *"For London session you can go with 20 contracts as filter"*

### Step 1 — Market State
- Price **inside** Value Area → **BALANCE** → this model is active
- *"Market state is consolidation — when the profile is protecting from breaking here and breaking here"*
- Uses the **previous day profile** as the balance reference
- *"You can make it stupid simple putting just daily profile"*

### Step 2 — Wait for First Breakout
- Do NOT take the first swing outside the range
- Wait for the first breakout, then wait for the retracement
- *"We are not trying to take the first swing because it's risky. We are getting the second swing."*
- *"I wait for the first breakout. You can use price action for this because I get clear market participants of what they want to do."*

### Step 3 — Location
- Same concept as trend following: find the **LVN** at the swing point of the retracement
- *"Same stuff — low volume node, aggression"*

### Step 4 — Trigger
- Big bubble in the **direction of returning inside the range**
- If price broke above the range → look for SELL aggression as it returns
- If price broke below the range → look for BUY aggression as it returns
- *"I wait for big trades, big orders — the common bubble of buy order that I can just say okay I jump in with them"*

### Take Profit
- Target = **POC of the balance area** (where maximum volume transacted)
- *"You go to where the bulk of the auctions taking place, where the probability that you will go to balance is really high"*
- NOT the extreme of the range — the POC is the high-probability target
- *"If you are wrong you want to be wrong immediately"*

### Stop Loss
- Immediately above/below the big bubble that triggered entry
- *"If you have big sell orders here immediately — here it's your stop loss"*
- Be wrong fast and with minimal loss

### Risk Management
- *"Immediately stop to break even"* after a small favorable move
- Aggressive break-even to protect capital
- *"If you take two stop loss, the stop loss is so small that you can afford to take three or four"*

---

## Empirical Discovery (Accidental)

During development of the trend following backtest, a bug in the TradeType convention
caused all signals to be executed in the **opposite direction** to the intended trend following logic.

Specifically, in `imbalance_up` zones (price above POC, outside VA), the system was entering
**SHORT** instead of LONG — fading the imbalance and targeting the Prev_POC below.

This accidentally implemented a mean reversion strategy: price is in imbalance, big sell
aggression appears (interpreted as trend following trigger but actually opposing), and the
trade targets the Prev_POC as the mean reversion destination.

**Results with the "wrong" convention (effectively mean reversion), filtered:**

| Filter | Trades | Win Rate | P&L | PF |
|--------|--------|----------|-----|----|
| ore=[11,12] + gex_short | 23 | 13.0% | +$1,992 | 2.44 |
| ore=[11,12] + backwardation | 72 | 19.4% | +$6,810 | 2.97 |
| ore=[11,12] (no macro filter) | 78 | 12.8% | +$1,017 | 1.24 |

These results were **significantly better** than the corrected trend following implementation,
suggesting that on ES futures the mean reversion dynamic is stronger than trend following
at the intraday level, particularly during hours 11-12.

**Important distinction from Fabio's mean reversion:**
- Fabio's model targets reversion from **outside** a balance range back to the POC inside
- The accidentally discovered version targets reversion from **imbalance** (outside VA) back to Prev_POC
- Both share the same underlying principle: price seeks balance, POC is the magnet

---

## Key Differences from Trend Following

| Aspect | Trend Following | Mean Reversion |
|--------|----------------|----------------|
| Market state | Imbalance (outside VA) | Balance (inside VA) |
| Entry direction | WITH the imbalance | AGAINST the imbalance |
| Session | NY RTH | London / summer NY |
| Target | Prev_POC (in trend direction) | Balance area POC |
| Productive hours | 9, 14 (A_LVN) | 11, 12 |
| GEX filter | Not useful | Short gamma helpful |
| VIX filter | Not useful | Backwardation helpful |

---

## Implementation Status

**Not yet implemented as a formal strategy runner.**

The mean reversion model is the next strategy to be built after validating
the trend following results on additional historical data.

When implementing, the starting point is the accidental discovery:
- Use `imbalance_up` zone + SELL aggression (TradeType=1, Volume >= threshold) → SHORT entry targeting Prev_POC
- Use `imbalance_down` zone + BUY aggression (TradeType=2, Volume >= threshold) → LONG entry targeting Prev_POC
- This is the mirror of the trend following signal direction
- Apply the same bar confirmation, entry at next_bar_open, and Prev_POC alignment filters

Then compare with Fabio's explicit mean reversion rules (balance + first breakout + retracement)
to understand if they produce the same or different signals.

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
TICKER    = "ES"
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

---

## Open Questions

1. Is the accidental discovery (imbalance + opposing aggression → revert to Prev_POC) the same
   as Fabio's explicit mean reversion model, or a different variant?
2. Does the model work better in London session as Fabio suggests, or the NY 11-12 window is sufficient?
3. Should the model use `Prev_POC` or the current session `POC` as target?
4. Is the second drive concept (wait for retracement after first breakout) already implicit in the
   LVN location filter, or does it need explicit implementation?
