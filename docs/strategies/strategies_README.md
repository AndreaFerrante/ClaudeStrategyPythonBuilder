# Strategy Knowledge Base

This folder contains documented knowledge for each trading strategy implemented or under research
in the Orderflow project. Each file is self-contained and should be read before implementing or
modifying the corresponding strategy runner.

## Available Strategies

### Valentini Trend Following
**File:** `valentini_trend_following.md`
**Status:** Implemented, backtested Jan 2025 – Feb 2026
**Runner:** `runner_data_valentini_backtest.py`

Three-step orderflow trend following model for ES RTH session. Enters in the direction of
imbalance at LVN locations using big order confirmation. Target is Prev_POC. Best results
with A_LVN variant filtered to hours 9 and 14.

---

### Valentini Mean Reversion
**File:** `valentini_mean_reversion.md`
**Status:** Research phase — not yet implemented as formal runner

Complementary model that fades imbalance back toward the POC. Works in balance/compression
conditions, primarily London session and summer months. Accidentally discovered during trend
following development — the "wrong" TradeType convention produced mean reversion signals that
outperformed the corrected trend following implementation. Next strategy to be built.

---

### Valentini Mean Reversion — Current POC Target
**File:** `valentini_mean_reversion_poc.md`
**Status:** Validated on ES with 9-month IS / 3-month OOS split (Jan 2025 – Dec 2025) + Jan 2026 sanity check
**Runner:** `runner_data_valentini_meanreversion_backtest.py` + `runner_trades_valentini_meanreversion_analysis.py`

Portable, language-agnostic spec of the **B_valley + current-POC + hour-10** configuration —
the only filter combination that survived a strict IS/OOS truth test with PF 3.78 in-sample
and PF 3.61 out-of-sample (4.5% drop). Fades aggressive single-direction prints outside the
Value Area when Prev_POC sits on the same dislocation side. TP = current session POC, SL =
2 ticks beyond the bubble's trigger price. All other filter candidates (contango, GEX regimes,
other hours) were rejected by the IS/OOS test. Designed to be implementable in any language /
platform with tick data and a Volume Profile.

---

## How to Add a New Strategy

1. Create `<strategyname>.md` in this folder following the existing structure:
   - Source / concept
   - Rules (session, entry, exit, filters)
   - Backtest results
   - Known issues
   - Open questions

2. Add an entry to this README

3. Reference the file in `CLAUDE.md` under the Strategy Knowledge Base section
