# Trend-Following Orderflow Strategy — Extracted

Synthesis from Valentini + FT71 + IVB/ORB + Grady. One concrete model.

## Name
**Initial Balance Breakout with Orderflow Confirmation (IBBO)**

## Core thesis
NY cash open = peak volume battle of day. First 30 min defines initial balance (IB). Winner of IB break controls auction. Join direction-of-pressure when orderflow confirms — not on first poke.

## Step 1 — Pre-session homework (FT71)
- Mark prior day **POC**, **VAH**, **VAL**, **session high/low**
- Mark **naked POCs** from last 3-5 sessions (untested high-volume nodes)
- Mark **LVN clusters** above/below current price (low-volume = unfair = fast travel)
- Note overnight high/low (96% chance day session breaks one)
- Bias check: is daily POC migrating up or down across last 5 days? Up = long bias, down = short bias

## Step 2 — Define Initial Balance
- Cash open: 09:30 ET (NQ/ES) or 08:30 CT
- IB = high/low of **first 30 minutes** of RTH
- Plot IB high (IBH) and IB low (IBL)
- Compute IB range. Skip day if IB range < 50% of 20-day avg IB (no volatility = no edge)

## Step 3 — Bias filter (Valentini)
Long only if ALL:
- Price > VWAP
- VWAP slope up
- CVD session positive AND rising
- Price accepted above prior day VAH OR breaking IBH

Short = mirror.

## Step 4 — Entry trigger (orderflow, Grady + Valentini)
Wait for IB break + retracement. On retest of IBH (long) or IBL (short):
- **Absorption**: large limit orders sit on bid (long) / offer (short), aggressive opposing market orders hit them, price does NOT move through. Stacked size that refreshes.
- **Aggression follow-through**: after absorption, see aggressive prints in trade direction (big_ask for long, big_bid for short) with momentum candle on M1
- **Delta confirmation**: bar CVD aligns with trade direction
- Enter on **15-second** bar close confirming direction (Valentini's execution TF), or M1 break-of-structure

Skip entry if:
- Volume drying up (lunch hour 12-13 ET)
- Spoofing detected (large order pulls before fill)
- Price chops back inside IB (failed breakout = fade signal, not trend)

## Step 5 — Stop placement
- Stop = beyond the absorption level by 1 ATR(M1) buffer
- Long: stop below the swing low that held during absorption
- Max risk: **0.25% account** per trade (Valentini base risk)
- If stop > 15 NQ points (or equivalent), skip — too wide, structure unclear

## Step 6 — Targets (statistical, deep stats model)
- **TP1**: first 1R-1.5R — scale 33% off, move stop to break-even
- **TP2**: prior day POC / next HVN / VWAP +2σ band — scale another 33%
- **TP3 (runner)**: prior day high/low, or prior session naked POC, or VWAP +3σ (7% prob hit per Valentini — don't anchor on it)

Target asymmetry rule (Valentini): if floating profit ≥ 3R and price reaches statistical extreme (+2σ from VWAP), **take it**. 3rd standard deviation hits only 7% of sessions.

## Step 7 — Trade management (FT71 + 20R session)
- After TP1 → stop to break-even
- Trail stop **below each new M1 swing low** (long) as auction extends
- If aggressive opposing orderflow appears (sellers absorbing buy aggression on long) AND M1 structure shifts → exit full remainder, don't wait for stop
- Never give back > 50% of peak unrealized P&L
- Time-stop: if entry + 30 min and price still inside entry zone with no MFE > 1R → flat

## Step 8 — Daily risk rules (Valentini)
- **Max 3 losses/day** → stop trading. Range day, not trend day
- **Max 1 active position** (no stacking)
- Compounding: after +3R on the day, can raise per-trade risk by 50% on remaining trades using day's profit
- No trades after 15:00 ET (closing rotation = mean-revert risk, not trend)

## Step 9 — Skip conditions
- IB range too small (< 50% avg)
- Inside prior day value with no edge break
- Holiday / pre-FOMC / pre-CPI 30 min window
- 3 consecutive failed IB break attempts (chop day)
- VWAP flat (slope near zero)

## Expected stats (from sources)
- Win rate: 50-60%
- Avg R:R: 1:2 to 1:3
- Trades/day: 1-3 (one good break, maybe retest)
- Monthly: positive expectancy assumes 15-20 trades

## Python implementation hooks (per CLAUDE.md schema)
Enrichment columns needed (already present):
- `IB_high`, `IB_low` → compute first 30 min RTH per session
- `vwap`, `vwap_slope`, `vwap_std_upper`, `vwap_std_lower` (1σ/2σ/3σ)
- `Session_CVD_rising` → bool
- `absorption_flag` → large refreshing limit + opposing aggression + price hold
- `big_ask` / `big_bid` (already enriched)
- `market_state` (already enriched: imbalance_up/down for bias)

Signal generation (long example):
```
signal = (
    (Datetime > IB_close_time)
    & (Price > IB_high)
    & (Price > vwap)
    & (vwap_slope > 0)
    & (Session_CVD > 0)
    & absorption_flag
    & big_ask
    & (market_state == "imbalance_up")
)
```

Entry = next_bar_open. Stop = absorption swing low - 1 ATR. TP ladder = [entry+1R, prior_POC, prior_high].

## One-line summary
Wait for IB break → retrace to break level → absorption holds → aggression confirms → enter with trend → scale at HVN/σ band → trail under M1 structure → exit on opposite orderflow shift.
