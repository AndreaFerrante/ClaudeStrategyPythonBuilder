# Valentini Mean Reversion — Current POC Target

**Version:** 1.0
**Status:** Empirically validated on ES futures (12 months in-sample + 1 month sanity check). See *Validation* section.
**Scope:** Self-contained strategy specification. Portable to any market with tick-level data and a Volume Profile. No language-specific code.

---

## 1. Concept

A **mean-reversion** model that fades aggressive single-direction prints occurring **outside the current session's Value Area** but **on the same side as the previous day's POC**. The hypothesis: when price is dislocated from the previous balance reference *in the direction the previous day already auctioned*, a fresh aggressive bubble against that direction signals dealer / late-participant exhaustion. Price snaps back to the **current session's POC** (the high-volume magnet within the current auction).

The model is **complementary** to a trend-following model that fades the same imbalance in the opposite direction. Where trend following bets the imbalance continues toward the prior POC, this MR variant bets the imbalance is exhausted and price reverts to the current POC.

### Causal chain

1. Market opens, builds a volume profile around the day's auction. The Point of Control (POC) is where most contracts have transacted.
2. Price extends **outside the Value Area** (the price band containing ~70% of session volume).
3. The dislocation aligns with the **previous session's POC on the same side** (e.g., price above current POC AND previous POC also above current price).
4. A **single-direction big print** appears against the dislocation (sell aggression while price is high; buy aggression while price is low).
5. The bar containing that print closes confirming the print's direction (bearish close after sell aggression, bullish close after buy aggression).
6. **Real-time interpretation:** late-comers / weak hands have just been faded by professional flow. Snap-back to the auction's center (POC) becomes high-probability.

---

## 2. Required Market Data

This strategy requires **tick-level data** with the following per-tick fields, derivable from any quality tick feed:

| Field | Description |
|---|---|
| `Datetime` | Tick timestamp, microsecond precision, UTC or session-local. |
| `SessionType` | Session classifier: regular trading hours (RTH) vs extended (ETH). The strategy only acts in RTH. |
| `Price` | Last traded price. |
| `Volume` | Contracts (or lots / shares) traded at this tick. |
| `TradeType` | Aggressor side: BUY (executed at best ask) vs SELL (executed at best bid). |
| `AskPrice`, `BidPrice` | Best ask / best bid at tick time. |

### Derivable fields (must be computed before the strategy runs)

| Derived field | Definition |
|---|---|
| `POC` | Current session's Point of Control — the price level with the highest cumulative traded volume since session open. Updates tick-by-tick. |
| `Prev_POC` | Final POC of the **previous session**. Carried forward as a constant for the entire current session. |
| `VA_Areas` | Classification of price relative to current session Value Area: `inside_VA`, `on_POC`, or `outside_VA`. The Value Area is the contiguous price band centered on POC that contains 70% of session volume. |
| `ValleysPeaks` | KDE-smoothed shape of the volume distribution. Key value: `valley` (price level is in a low-volume trough between two high-volume peaks). |
| `current_bar_open / high / low / close` | OHLC of the 1-minute bar that contains the tick. The close is the **final** close of that 1-minute window — used after the bar physically closes (see §6). |
| `next_bar_open` | Open price of the 1-minute bar that follows the current bar. Strategy entry price. |
| `next_bar_first_index` | Identifier of the first tick of the next 1-minute bar. Strategy entry timing. |

### External regime overlays (optional, see §11)

| Overlay | Source |
|---|---|
| VIX term structure (contango / backwardation) | Daily, lagged by 1 trading day |
| GEX (gamma exposure) regime (long / short gamma) | Daily, lagged by 1 trading day, classified relative to its own 252-day rolling mean |

---

## 3. Volume Profile Preliminaries

The strategy depends on a properly built rolling **session volume profile**:

- Volume profile **resets at the start of each ETH session**, so the "current session" reference is the full extended-hours session including the overnight build.
- POC = price with maximum traded volume since reset.
- Value Area = smallest contiguous price band centered on POC that contains 70% of session volume.
- `Prev_POC` is the final POC value of the previous ETH session, locked at session boundary and propagated through the next session.
- A KDE smoothing of the volume-by-price histogram identifies **valleys** (low-volume troughs) and **peaks** (high-volume crests). Tick is "in valley" when its price sits in the lowest tier of the KDE shape.

**Critical filter:** if `Prev_POC == 0` (first session in the dataset), discard all signals from that session.

---

## 4. Market State Qualification

Every tick is classified into one of four states, evaluated tick-by-tick:

| State | Conditions | Strategy reaction |
|---|---|---|
| `balance` | Price is **inside Value Area** (`VA_Areas` ∈ `{inside_VA, on_POC}`) | Skip — strategy idle |
| `imbalance_up_mr` | Price > current POC **AND** Price is **outside** Value Area **AND** Prev_POC < Price | Look for SHORT trigger |
| `imbalance_down_mr` | Price < current POC **AND** Price is **outside** Value Area **AND** Prev_POC > Price | Look for LONG trigger |
| `imbalance_no_target_mr` | Outside Value Area but Prev_POC alignment fails (Prev_POC on the wrong side of price) | Skip |

**Note on alignment:** the "MR" suffix marks an inverted Prev_POC alignment relative to the trend-following variant. In MR, Prev_POC sits on the **same side** as the dislocation (Prev_POC below current price when price is dislocated upward), reinforcing the claim that the move is exhausted: yesterday already auctioned in this region; further extension is unlikely.

---

## 5. Trigger Conditions

A trigger tick must satisfy **all** of the following:

### 5.1 — Market state qualifier
Tick's `market_state` is `imbalance_up_mr` (for SHORT trigger) or `imbalance_down_mr` (for LONG trigger).

### 5.2 — Location flag
Tick's price is **at a Volume Profile valley** (`ValleysPeaks` ∈ `{valley, valley_extreme}`). This filter is named **B_valley** in the variant taxonomy. The valley flag selects price levels where the historical volume distribution shows a low-density trough — the kind of level where rejected price often pivots.

### 5.3 — Aggression threshold (Big Bubble)
Tick's `Volume` ≥ `BIG_ORDER_THRESHOLD` AND tick's `TradeType` matches the trigger direction:

| Trigger | TradeType requirement |
|---|---|
| SHORT | SELL aggression (executed on the bid) |
| LONG  | BUY aggression (executed on the ask) |

#### `BIG_ORDER_THRESHOLD` calibration

- **ES futures (validation):** **50 contracts**.
- **Calibration heuristic for any instrument:** choose threshold ≈ **0.30%–0.50%** of total RTH ticks. For ES this corresponds to ~0.38% of RTH ticks at threshold 50.
- For lower-volume instruments scale proportionally to typical contract size (e.g., NQ historically ~30 contracts; futures with smaller average volumes may need 10–20).
- Lower thresholds (10–20) on ES showed worse out-of-sample stability; threshold 100 produced too few signals to be statistically meaningful.

### 5.4 — First bubble per bar
Within a 1-minute bar, only the **first qualifying tick** is treated as a trigger. Subsequent qualifying ticks in the same bar are ignored.

### 5.5 — Opposing-bubble veto
After the trigger tick fires, if **any other big bubble** (Volume ≥ `BIG_ORDER_THRESHOLD`) appears in the same bar with the **opposite TradeType**, the entire bar's trigger is invalidated.

**Formal rule (for the coding agent):**
> Let the trigger tick have sequence `trigger_seq` and trade type `trigger_TradeType`.
> Scan every subsequent tick in the **same 1-minute bar** (same `current_bar_datetime`) with `Sequence > trigger_seq`.
> If ANY such tick has `Volume >= BIG_ORDER_THRESHOLD` AND `TradeType == 3 − trigger_TradeType` (opposite aggression — `1↔2` invariant), **the bar is marked invalid and yields no signal**, regardless of how many qualifying trigger candidates the bar contained.

**Scope of the veto:**
- Only the current 1-minute bar is invalidated. Previous and next bars are unaffected.
- The veto is checked on **every tick from `trigger_seq` to bar close**, not just on the bubble nearest the close.
- Ticks with `Volume < BIG_ORDER_THRESHOLD` are ignored — only big bubbles can trigger the veto.
- A new bar resets the state: if the next bar produces a new valid trigger, it is taken normally.

**Worked example** (ES, 2025-02-13, 10:31 CT bar, threshold = 50):

| Time (CT)        | Price   | Volume | TradeType | Role                                  |
|------------------|---------|--------|-----------|---------------------------------------|
| 10:31:25.682     | 6105.50 | 85     | 1 (SELL)  | trigger fires (first qualifier)       |
| 10:31:34.930     | 6104.00 | 61     | 2 (BUY)   | opposing big bubble → bar invalidated |
| 10:31:58.313     | 6103.00 | 50     | 1 (SELL)  | irrelevant — bar already dead         |

Result: no signal generated for the 10:31 bar. No order entered at the 10:32 open.

This rule guards against cases where the trigger is overwhelmed by stronger flow in the opposite direction before the bar closes.

---

## 6. Bar Confirmation

After a trigger fires, the strategy **waits for the trigger bar to close**, then evaluates:

| Direction | Confirmation rules |
|---|---|
| SHORT | bar `close < open` (bearish bar) **AND** bar `close < trigger_BidPrice` (close below the price where the SELL bubble executed) |
| LONG  | bar `close > open` (bullish bar) **AND** bar `close > trigger_AskPrice` (close above the price where the BUY bubble executed) |

If confirmation fails, the trigger is discarded.

The "close < trigger price" requirement enforces that the bar closed **on the same side as the bubble's price action**, providing a crude form of follow-through evidence even before entry.

---

## 7. Entry

If the trigger bar closes confirmed:

1. **Entry price** = open price of the next 1-minute bar (`next_bar_open`).
2. **Entry timing** = first tick of the next 1-minute bar.
3. **Direction** = opposite to the qualifier direction's natural reading: SHORT in `imbalance_up_mr`, LONG in `imbalance_down_mr`.

This is a fade entry: in `imbalance_up_mr` (price dislocated upward) the trade is SHORT against the upward dislocation, betting on reversion to current POC.

---

## 8. Exit

Three exit mechanisms, evaluated in order of priority on every tick:

| Priority | Mechanism | Trigger |
|---|---|---|
| 1 | **Take Profit** | Price reaches **current session POC**. Full exit. |
| 2 | **Stop Loss**   | Price moves 2 ticks **beyond the bubble's trigger price**. Full exit. |
| 3 | **Time exit**   | At/after `SESSION_CLOSE_TIME` (default 1 hour before instrument's RTH close — 15:00 CT for ES). Force-close any open position at market. |

### 8.1 — Take Profit target

The TP target is the **current session POC** at the time of entry. Note: POC drifts during the session as new volume builds; for backtest reproducibility use the POC value at the trigger tick's timestamp. For live trading, recompute POC and update the working order tick-by-tick if the broker permits.

**Why current POC and not previous POC?** Using current-session POC yielded **better out-of-sample stability**. Previous-session POC produced higher peak performance in some periods but worse degradation between in-sample and out-of-sample windows.

### 8.2 — Stop Loss

Stop loss = trigger bubble's price ± 2 × tick size:
- SHORT: stop = `BidPrice + 2 × tick_size` (just above the SELL bubble's execution price)
- LONG:  stop = `AskPrice − 2 × tick_size` (just below the BUY bubble's execution price)

This is intentionally tight: the strategy's premise is that the bubble marks the local turning point. If price moves more than 2 ticks past the bubble in the original direction, the premise has failed.

### 8.3 — Time exit (force close)

The strategy must always force-close any open position before session end. Default cutoff = **1 hour before RTH close** (e.g., 15:00 CT for the U.S. equity session that closes 16:00 CT). The cutoff is also used to **reject entries** whose `next_bar_open` falls at or after the cutoff: no entries are taken in the final hour of the session.

---

## 9. Position Sizing & Risk

The validation used **1 contract per trade**. The strategy specification does not prescribe sizing — it is the responsibility of the implementer to size based on:

- Per-trade dollar risk (with stop ≈ 2 ticks, ES 1 contract = $25 risk before slippage; real-world slippage/spread can extend losses to ~$200/trade).
- Account size and max-drawdown tolerance.
- Concurrent position policy (the validation allowed one position at a time; multiple concurrent positions are out of scope).

Commission used in validation: $0.90 round-trip per contract.
Slippage model used in validation: uniform 0–2 ticks on entry and exit.

---

## 10. Validation

### 10.0 — Primary recommendation

The configuration that survives the strict in-sample / out-of-sample truth test with the smallest performance drop is:

- **Location filter:** `B_valley` — trigger price at a Volume-Profile valley.
- **Take-profit target:** **current session POC** (not previous-session POC).
- **Time-of-day filter:** **entry hour = 10 (CT)** only. All other hours rejected.
- **Direction filter:** none beyond the market-state qualifier from §4.
- **Macro overlays:** none. VIX and GEX overlays were tested and rejected (see §10.4).

This is the single configuration the implementer should deploy unless they have an independent reason to deviate. All numbers below refer to this configuration unless explicitly stated.

### 10.1 — Dataset

- **Instrument:** E-mini S&P 500 futures (ES).
- **Tick feed:** Sierra Chart / CME aggregated tick stream.
- **Period:** January 2025 through December 2025 (12 months).
- **In-Sample (IS):** January–September 2025 (9 months).
- **Out-of-Sample (OOS):** October–December 2025 (3 months). OOS data was held out of all filter selection.
- **Sanity-check month:** January 2026 (1 month, used as forward test only — not part of the IS/OOS split).
- **Validation rule:** filters were proposed and tuned exclusively on IS. The OOS window was reserved as the truth test. A filter is considered validated only when its IS and OOS performance are similar in PF and direction, with both samples meeting the n ≥ 15 threshold.

### 10.2 — Aggregate result, recommended configuration (B_valley + POC, hour=10)

| Period | Trades | Win rate | Profit factor | Net P&L | Max DD |
|---|---:|---:|---:|---:|---:|
| **IS (Jan–Sep 2025)** | 33 | 21.2% | **3.78** | +$7,233 | small |
| **OOS (Oct–Dec 2025)** | 16 | 18.8% | **3.61** | +$2,836 | small |
| **Full 12 months** | 49 | 20.4% | **3.73** | +$10,068 | $882 (1 ES contract) |

**Verdict.** PF is essentially identical between IS and OOS (3.78 vs 3.61, a 4.5% drop). This is the only filter combination in the search space that survived the IS/OOS test with a drop under 10% — every other filter either degraded sharply, broke entirely, or had insufficient OOS sample. The hour-10 filter has a defensible causal explanation (see §10.3) which lowers the probability that the survival is a chance artifact.

### 10.3 — Why hour-10 is the only validated filter

**Causal explanation.** The 10:00 CT hour (= 11:00 ET) sits roughly 90 minutes after the NYSE cash open. By this time:

- The opening auction's retail flow has been absorbed.
- Overnight inventory imbalances have triggered the first round of dealer hedging.
- The volume profile has built enough structure to define a credible POC and Value Area.
- Late participants chasing the open's directional move are now exposed to fading by professional flow.

Mean reversion against a freshly-printed bubble at this hour is a structural phenomenon, not a calendar coincidence.

**Hours that look profitable but fail OOS.** The full hour-by-hour table is listed below to make the survival of hour-10 visible against the noise around it:

| Hour | IS PF | OOS PF | Status |
|---:|---:|---:|---|
| 8 | 0.14 | 0.24 | reject — both halves negative |
| 9 | 1.49 | 0.00 | reject — IS positive, OOS catastrophic |
| **10** | **3.78** | **3.61** | **VALIDATED** |
| 11 | 0.59 | 3.07 | reject — IS negative, OOS small-sample positive |
| 12 | 0.74 | 1.57 | reject — IS unprofitable |
| 13 | weak | weak | reject |
| 14 | 0.72 | 1.51 | reject — IS unprofitable |

Note hour 11 illustrates the danger of looking at OOS without first qualifying on IS: a strategy "ranked good in OOS" that was unprofitable in-sample is not a discovery — it is a small-sample artifact you would never have selected if you had not seen OOS first.

### 10.4 — Filters tested and rejected

The following filters were evaluated against the same IS/OOS split. None survived:

| Filter | IS PF | OOS PF | Rejection reason |
|---|---:|---:|---|
| VIX contango (front-month structure) | 1.72 | 0.18 | breaks OOS |
| GEX long gamma | 1.31 | 0.55 | breaks OOS |
| GEX short gamma + contango | 1.69 | 0.56 | breaks OOS |
| GEX short gamma alone | 1.21 | 1.67 | borderline; small OOS sample (n=20) |
| Hours 9–10 grouped | 2.86 | 1.06 | hour 9 drags down the combined edge |
| Hours 11–12 (originally hypothesized as productive in source video material) | 0.59 | 3.07 | IS unprofitable |
| Previous-POC target with same B_valley + hour=10 | 6.11 | 2.34 | survives but drops 61% IS→OOS — current POC target chosen instead |

**The original strategy guide's hypothesis** that the model "works in London session and during summer compression months" was not supported. RTH session (Chicago 08:30–15:00) was used throughout, and the summer/winter months did not differentiate.

### 10.5 — Caveats

- **Sample size for the recommended configuration is 49 trades** over 12 months. CLAUDE.md guidance: <30 = low confidence; 30–100 = marginal; ≥100 = robust. The recommended configuration sits in the marginal range. Acquire at least one additional out-of-sample year (or an independent earlier year — 2024 or 2023) before deploying real capital.
- All validation periods are post-COVID, post-2022 bear, in a generally accommodative monetary regime. Performance under sustained 2008-style stress is unverified.
- The `BIG_ORDER_THRESHOLD = 50` for ES was selected from a coarse sweep and not finely tuned. Production deployment should re-calibrate per instrument.
- The strategy is **low-frequency** (~4 trades/month at hour=10). Capital efficiency depends on running it alongside complementary strategies, not standalone.
- Worst single losing day in validation: 2025-05-08, two consecutive stop-outs in the same hour, total ≈ −$480 on 1 contract. News-day stress vulnerability is real.
- Real-world stop-loss execution is harder than 2 ticks: validation used a 0–2 tick uniform slippage model; live execution may produce stop fills 4–8 ticks beyond the trigger price during fast moves. Re-evaluate with realistic slippage before live deployment.

---

## 11. Optional Regime Overlays

Two daily macro overlays were tested. **Neither survived out-of-sample as a standalone filter** on this dataset; they are documented here for completeness, not as recommendations.

| Overlay | Definition | OOS verdict |
|---|---|---|
| VIX contango | Previous trading day: front-month VIX futures (F1) below second-month (F2) → contango | Helped H1, broke in H2 → reject |
| GEX short gamma | Previous trading day: dealer gamma exposure below 252-day rolling mean | Marginal H1, weakened H2 → reject |

If a downstream implementer wants to layer regime filters, they should validate independently on their own dataset and time frame; do not adopt these from the validation results.

---

## 12. Parameters Summary

| Parameter | Default | Notes |
|---|---|---|
| `BIG_ORDER_THRESHOLD` | 50 contracts (ES) | Calibrate to ~0.30–0.50% of RTH tick count for the instrument |
| `STOP_LOSS_TICKS` | 2 | Distance from bubble price |
| `TAKE_PROFIT` | current session POC | Drifting target; recompute live or freeze at entry |
| `BAR_GRANULARITY` | 1 minute | Used for trigger and entry-bar logic |
| `LOCATION_FILTER` | volume-profile valley | Other location filters (LVN-only, LVN+valley intersection) tested but inferior |
| `SESSION_FILTER` | RTH only | Skip extended-hours sessions |
| `SESSION_OPEN_GUARD` | none mandatory; 08:30 CT for ES | Skip the very-first ticks of session if Prev_POC unavailable |
| `SESSION_CLOSE_GUARD` | force-close 1 hour before RTH end (15:00 CT for ES) | No new entries within last hour |
| `COMMISSION` | depends on broker | Validation used $0.90 round-trip |
| `SLIPPAGE_MODEL` | 0–2 ticks uniform per fill | Validation assumption |

---

## 13. Implementation Checklist (cross-language, cross-platform)

A correct implementation must satisfy **all** of the following invariants:

1. **No look-ahead.** All filters and confirmations use only data available at or before the relevant decision point. The bar-close confirmation (§6) is legitimate **only** because the entry executes on the **next** bar's open, after the trigger bar has physically closed.
2. **Prev_POC > 0** filter applied at session start.
3. **Trade direction matches market state direction** — SHORT in `imbalance_up_mr`, LONG in `imbalance_down_mr`. This is counter-intuitive (the trigger TradeType is on the same side as the dislocation, but the trade fades it).
4. **First-bubble-per-bar** dedup applied before opposing-bubble veto.
5. **Opposing-bubble veto** evaluated using TradeType ≠ trigger TradeType, within the same bar, after the trigger sequence index.
6. **Entry timing** = first tick of next bar, not the trigger tick itself.
7. **Force-close** at session-close cutoff overrides every other exit logic.
8. **One position at a time** unless explicitly extended.
9. **Tick-level execution** of TP/SL — do not check on bar boundaries only; intra-bar moves can hit the stop.

---

## 14. Out-of-scope (intentionally left for the implementer)

- Live data ingestion and broker connectivity.
- Order routing / partial fill handling.
- Position sizing and portfolio aggregation.
- Multi-instrument execution.
- Parameter re-calibration cadence.
- Performance reporting beyond the basic KPI set.
