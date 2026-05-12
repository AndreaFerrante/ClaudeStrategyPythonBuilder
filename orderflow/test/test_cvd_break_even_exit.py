"""
Tests for CVDBreakEvenExit.

Each test drives a minimal BacktestEngine run with synthetic tick data so that
the full engine loop (entry mask, on_entry, on_tick, TradeRecord assembly) is
exercised end-to-end — no mocking of internals.

Tick data layout
----------------
Row 0 : entry tick (matched by signal Index=0)
Rows 1-N : in-trade ticks with controlled Price and CVD values

Signal layout
-------------
A single signal with Index=0 sets entry at the price of row 0.
TP and SL are set wide so DynamicTPSLExit never fires in BE tests.
"""

import numpy as np
import pandas as pd
import pytest

from orderflow.backtester.engine import BacktestEngine
from orderflow.backtester.execution import SlippageModel, SlippageMode
from orderflow.backtester.exits import (
    CVDBreakEvenExit,
    CompositeExit,
    DynamicTPSLExit,
    HourBasedExit,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TICK_SIZE = 0.25
ENTRY_PRICE = 5000.0


def _make_engine() -> BacktestEngine:
    return BacktestEngine(
        tick_size=TICK_SIZE,
        tick_value=12.5,
        commission=0.0,
        n_contracts=1,
        slippage_model=SlippageModel(mode=SlippageMode.FIXED, max_ticks=0),
        progress_bar=False,
    )


def _build_tick_data(prices: list[float], cvd_values: list[float]) -> pd.DataFrame:
    """Create a minimal tick DataFrame understood by the engine."""
    n = len(prices)
    datetimes = pd.date_range("2025-01-02 09:00:00", periods=n, freq="1s")
    return pd.DataFrame(
        {
            "Index": np.arange(n, dtype=np.int64),
            "Datetime": datetimes,
            "Date": [d.date() for d in datetimes],
            "Time": [d.time() for d in datetimes],
            "Price": prices,
            "SessionType": ["RTH"] * n,
            "CVD": cvd_values,
        }
    )


def _build_signal(
    trade_type: int,
    baseline_cvd: float,
    tp_ticks: float = 100.0,
    sl_ticks: float = 100.0,
) -> pd.DataFrame:
    """
    Single signal at Index=0.
    trade_type: 1 = SHORT, 2 = LONG  (engine convention)
    """
    return pd.DataFrame(
        {
            "Index": [0],
            "TradeType": [trade_type],
            "CVD": [baseline_cvd],
            "TP_Ticks": [tp_ticks],
            "SL_Ticks": [sl_ticks],
        }
    )


# ---------------------------------------------------------------------------
# Test 1: Long — BE activates when both gates open, price retraces to BE stop
# ---------------------------------------------------------------------------

def test_long_be_activates_and_stops() -> None:
    """
    Long trade: CVD confirms only after min_profit_ticks are in place.
    Price then retraces to entry → exit at break-even.
    """
    baseline_cvd = 100.0
    min_profit_ticks = 4

    # Tick sequence:
    #  0 → entry at 5000  (CVD=100)
    #  1 → price 5000.75 (3 ticks up), CVD=200 — profit gate NOT met → no activation
    #  2 → price 5001.00 (4 ticks up), CVD=200 — BOTH gates met → BE activated at 5000
    #  3 → price 5000.00 → hits BE stop → exit
    prices = [ENTRY_PRICE, 5000.75, 5001.00, ENTRY_PRICE]
    cvd = [baseline_cvd, 200.0, 200.0, 200.0]

    data = _build_tick_data(prices, cvd)
    signals = _build_signal(trade_type=2, baseline_cvd=baseline_cvd)  # 2 = LONG

    engine = _make_engine()
    cvd_be = CVDBreakEvenExit(
        signals_df=signals,
        cvd_col="CVD",
        min_profit_ticks=min_profit_ticks,
        cvd_delta_threshold=0.0,
        offset_ticks=0.0,
        tick_size=TICK_SIZE,
    )
    exit_strategy = CompositeExit([
        DynamicTPSLExit(signals_df=signals, tick_size=TICK_SIZE),
        cvd_be,
        HourBasedExit(close_hour=23, close_minute=0),
    ])

    result = engine.run(
        data=data,
        signals=signals,
        exit_strategy=exit_strategy,
        indicator_columns=["CVD"],
    )

    assert len(result.trades_df) == 1, "Expected exactly one trade"
    trade = result.trades_df.iloc[0]
    assert trade["exit_reason"] == "break_even", f"Expected break_even, got {trade['exit_reason']}"
    assert trade["exit_price"] == ENTRY_PRICE, f"Expected exit at {ENTRY_PRICE}, got {trade['exit_price']}"
    assert trade["break_even_activated"] is True or trade["break_even_activated"] == True


# ---------------------------------------------------------------------------
# Test 2: Short — mirror of Test 1
# ---------------------------------------------------------------------------

def test_short_be_activates_and_stops() -> None:
    """
    Short trade: CVD declines only after min_profit_ticks are in place.
    Price retraces to entry → exit at break-even.
    """
    baseline_cvd = 100.0
    min_profit_ticks = 4

    # Tick sequence:
    #  0 → entry at 5000  (CVD=100)
    #  1 → price 4999.25 (3 ticks down), CVD=0 — profit gate NOT met → no activation
    #  2 → price 4999.00 (4 ticks down), CVD=0 — BOTH gates met → BE activated at 5000
    #  3 → price 5000.00 → hits BE stop → exit
    prices = [ENTRY_PRICE, 4999.25, 4999.00, ENTRY_PRICE]
    cvd = [baseline_cvd, 0.0, 0.0, 0.0]

    data = _build_tick_data(prices, cvd)
    signals = _build_signal(trade_type=1, baseline_cvd=baseline_cvd)  # 1 = SHORT

    engine = _make_engine()
    cvd_be = CVDBreakEvenExit(
        signals_df=signals,
        cvd_col="CVD",
        min_profit_ticks=min_profit_ticks,
        cvd_delta_threshold=0.0,
        offset_ticks=0.0,
        tick_size=TICK_SIZE,
    )
    exit_strategy = CompositeExit([
        DynamicTPSLExit(signals_df=signals, tick_size=TICK_SIZE),
        cvd_be,
        HourBasedExit(close_hour=23, close_minute=0),
    ])

    result = engine.run(
        data=data,
        signals=signals,
        exit_strategy=exit_strategy,
        indicator_columns=["CVD"],
    )

    assert len(result.trades_df) == 1
    trade = result.trades_df.iloc[0]
    assert trade["exit_reason"] == "break_even", f"Expected break_even, got {trade['exit_reason']}"
    assert trade["exit_price"] == ENTRY_PRICE
    assert trade["break_even_activated"] is True or trade["break_even_activated"] == True


# ---------------------------------------------------------------------------
# Test 3: CVD never confirms — trade exits via TP, break_even_activated=False
# ---------------------------------------------------------------------------

def test_no_cvd_confirmation_no_be() -> None:
    """
    CVD stays flat → BE never activates.
    Trade hits TP via DynamicTPSLExit.
    break_even_activated must be False.
    """
    baseline_cvd = 100.0
    tp_ticks = 4

    prices = [ENTRY_PRICE, 5000.50, 5001.00]  # 4 ticks up = TP hit at tick 2
    cvd = [baseline_cvd, baseline_cvd, baseline_cvd]  # no CVD movement

    data = _build_tick_data(prices, cvd)
    signals = _build_signal(trade_type=2, baseline_cvd=baseline_cvd, tp_ticks=tp_ticks, sl_ticks=100.0)

    engine = _make_engine()
    cvd_be = CVDBreakEvenExit(
        signals_df=signals,
        cvd_col="CVD",
        min_profit_ticks=2,
        cvd_delta_threshold=50.0,  # threshold never reached (CVD flat)
        tick_size=TICK_SIZE,
    )
    exit_strategy = CompositeExit([
        DynamicTPSLExit(signals_df=signals, tick_size=TICK_SIZE),
        cvd_be,
        HourBasedExit(close_hour=23, close_minute=0),
    ])

    result = engine.run(
        data=data,
        signals=signals,
        exit_strategy=exit_strategy,
        indicator_columns=["CVD"],
    )

    assert len(result.trades_df) == 1
    trade = result.trades_df.iloc[0]
    assert trade["exit_reason"] == "take_profit", f"Expected take_profit, got {trade['exit_reason']}"
    assert trade["break_even_activated"] is False or trade["break_even_activated"] == False


# ---------------------------------------------------------------------------
# Test 4: Profit gate blocks activation even when CVD confirms immediately
# ---------------------------------------------------------------------------

def test_profit_gate_blocks_early_activation() -> None:
    """
    CVD immediately confirms direction but price hasn't moved enough.
    Profit gate prevents BE activation.
    Trade eventually hits SL → exit as stop_loss, break_even_activated=False.
    """
    baseline_cvd = 100.0
    sl_ticks = 4
    min_profit_ticks = 8  # high threshold — price never reaches it

    # Price rises 2 ticks (below gate), then falls to SL
    sl_price = ENTRY_PRICE - sl_ticks * TICK_SIZE  # 4999.00
    prices = [ENTRY_PRICE, 5000.50, sl_price]
    cvd = [baseline_cvd, 500.0, 500.0]  # CVD confirms immediately but price gate not met

    data = _build_tick_data(prices, cvd)
    signals = _build_signal(trade_type=2, baseline_cvd=baseline_cvd, tp_ticks=100.0, sl_ticks=sl_ticks)

    engine = _make_engine()
    cvd_be = CVDBreakEvenExit(
        signals_df=signals,
        cvd_col="CVD",
        min_profit_ticks=min_profit_ticks,
        cvd_delta_threshold=0.0,
        tick_size=TICK_SIZE,
    )
    exit_strategy = CompositeExit([
        DynamicTPSLExit(signals_df=signals, tick_size=TICK_SIZE),
        cvd_be,
        HourBasedExit(close_hour=23, close_minute=0),
    ])

    result = engine.run(
        data=data,
        signals=signals,
        exit_strategy=exit_strategy,
        indicator_columns=["CVD"],
    )

    assert len(result.trades_df) == 1
    trade = result.trades_df.iloc[0]
    assert trade["exit_reason"] == "stop_loss", f"Expected stop_loss, got {trade['exit_reason']}"
    assert trade["break_even_activated"] is False or trade["break_even_activated"] == False
