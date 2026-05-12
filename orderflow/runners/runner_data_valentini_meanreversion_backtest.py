"""
Mean Reversion runner — Valentini model (mirror of Trend Following).

Step A reused from runner_data_valentini_trend_following_backtest.py
(filter_operative_session, add_cvd_directional, add_aggression_flag,
add_prev_session_range). Only `add_market_state_mr` is changed: Prev_POC
alignment is INVERTED so that the geometry of the fade is consistent.

Step B mirrors TF: the TradeType condition is flipped (SELL agg in
imbalance_up_mr → SHORT, BUY agg in imbalance_down_mr → LONG). Bar
confirmation, opposing-bubble filter, entry at next_bar_open and stop
loss rules are identical to TF (mirrored automatically by signal_direction).

Two TP variants are emitted per location: target = Prev_POC and target =
current session POC. Output naming: trades_MR_<variant>_<target>_<YYYYMM>.parquet
"""

import gc
from pathlib import Path

import polars as pl

import orderflow.configuration as cf

from orderflow.backtester.execution import SlippageMode, SlippageModel
from orderflow.backtester.engine import BacktestEngine
from orderflow.backtester.exits import CompositeExit, DynamicTPSLExit, HourBasedExit

pl.Config.set_tbl_rows(100)

# region ------------------------------- CONFIG ----------------------------------------
FOLDER  = r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/ES/parquet/"
OUT_DIR = Path(r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/ES/Trades/Valentini/mirror_reverting")
TICKER  = "ES"
MONTHS  = [
    "202501", "202502", "202503", "202504", "202505", "202506",
    "202507", "202508", "202509", "202510", "202511", "202512",
]

tick_size = cf.FUTURE_VALUES.loc[cf.FUTURE_VALUES['Ticker'] == TICKER, 'Tick_Size'].values[0]
tick_value = cf.FUTURE_VALUES.loc[cf.FUTURE_VALUES['Ticker'] == TICKER, 'Tick_Value'].values[0]

engine = BacktestEngine(
    tick_size=tick_size,
    tick_value=tick_value,
    commission=0.9,
    n_contracts=1,
    slippage_model=SlippageModel(mode=SlippageMode.UNIFORM, max_ticks=2, seed=42),
    progress_bar=False,
)

SESSION_START_HOUR = 8
SESSION_START_MINUTE = 30
SESSION_END_HOUR = 15
SESSION_END_MINUTE = 0
BIG_ORDER_THRESHOLD = 50
STOP_TICKS = 2
TICK_SIZE = tick_size

COLUMNS_NEEDED = [
    "Index", "Sequence", "Date", "Time", "Datetime",
    "Hour", "SessionType", "Prev_POC",
    "TradeType", "Volume", "AskPrice", "BidPrice", "Price",
    "VA_Areas", "POC", "Session_High", "Session_Low",
    "LVN", "ValleysPeaks",
    "CD_Ask", "CD_Bid",
    "current_bar_askvolume", "current_bar_bidvolume",
    "Node_Ask_Volume", "Node_Bid_Volume",
    "current_bar_datetime", "current_bar_open", "current_bar_high",
    "current_bar_low", "current_bar_close",
    "next_bar_datetime", "next_bar_open", "next_bar_high",
    "next_bar_low", "next_bar_close",
]

LOCATION_VARIANTS = {
    "A_LVN":        ("loc_A", "LVN solo"),
    "B_valley":     ("loc_B", "ValleysPeaks (-2,-1) solo"),
    "C_LVN_valley": ("loc_C", "LVN AND ValleysPeaks (-2,-1)"),
}

TARGET_VARIANTS = {
    "PrevPOC": "Prev_POC",
    "POC":     "POC",
}
# endregion


# region ------------------------------- STEP A ----------------------------------------
def filter_operative_session(df: pl.DataFrame) -> pl.DataFrame:
    """RTH window 08:30-15:00 CT, Prev_POC > 0."""
    return (
        df
        .filter(pl.col("SessionType") == "RTH")
        .filter(pl.col("Prev_POC") > 0)
        .with_columns(pl.col("Datetime").dt.minute().alias("_minute"))
        .filter(
            ((pl.col("Hour") == SESSION_START_HOUR) & (pl.col("_minute") >= SESSION_START_MINUTE))
            | (pl.col("Hour") > SESSION_START_HOUR)
        )
        .filter(pl.col("Hour") < SESSION_END_HOUR)
        .drop("_minute")
    )


def add_cvd_directional(df: pl.DataFrame) -> pl.DataFrame:
    """CVD (session), bar CVD, node CVD, signed tick_delta."""
    return df.with_columns(
        (pl.col("CD_Bid") - pl.col("CD_Ask")).alias("CVD"),
        (pl.col("current_bar_bidvolume") - pl.col("current_bar_askvolume")).alias("current_bar_cvd"),
        (pl.col("Node_Bid_Volume") - pl.col("Node_Ask_Volume")).alias("node_cvd"),
        pl.when(pl.col("TradeType") == 2).then(pl.col("Volume"))
          .when(pl.col("TradeType") == 1).then(-pl.col("Volume"))
          .otherwise(0).alias("tick_delta"),
    )


def add_market_state_mr(df: pl.DataFrame) -> pl.DataFrame:
    """
    Mean Reversion market state — Prev_POC alignment INVERTED vs TF (R1).

      imbalance_up_mr:   outside VA, Price > POC, Prev_POC < Price
                         → fade SHORT toward Prev_POC (below) or POC (below)
      imbalance_down_mr: outside VA, Price < POC, Prev_POC > Price
                         → fade LONG  toward Prev_POC (above) or POC (above)
      balance / imbalance_no_target_mr → skip
    """
    return df.with_columns(
        pl.when(pl.col("VA_Areas").is_in(["VA", "PO"]))
        .then(pl.lit("balance"))
        .when(
            (pl.col("VA_Areas") == "na")
            & (pl.col("Price") > pl.col("POC"))
            & (pl.col("Prev_POC") < pl.col("Price"))
        ).then(pl.lit("imbalance_up_mr"))
        .when(
            (pl.col("VA_Areas") == "na")
            & (pl.col("Price") < pl.col("POC"))
            & (pl.col("Prev_POC") > pl.col("Price"))
        ).then(pl.lit("imbalance_down_mr"))
        .otherwise(pl.lit("imbalance_no_target_mr"))
        .alias("market_state")
    )


def add_aggression_flag(df: pl.DataFrame, threshold: int = BIG_ORDER_THRESHOLD) -> pl.DataFrame:
    """big_ask = BUY agg (TT=2), big_bid = SELL agg (TT=1), each gated by Volume >= threshold."""
    return df.with_columns(
        ((pl.col("TradeType") == 2) & (pl.col("Volume") >= threshold)).alias("big_ask"),
        ((pl.col("TradeType") == 1) & (pl.col("Volume") >= threshold)).alias("big_bid"),
    )


def add_prev_session_range(df: pl.DataFrame) -> pl.DataFrame:
    """Range (High-Low) of the previous ETH session, propagated to every tick after it."""
    return (
        df
        .with_columns(
            ((pl.col("SessionType") == "ETH") & (pl.col("SessionType").shift(-1) != "ETH"))
            .alias("_is_last_eth_tick")
        )
        .with_columns((pl.col("Session_High") - pl.col("Session_Low")).alias("_session_range"))
        .with_columns(
            pl.when(pl.col("_is_last_eth_tick"))
              .then(pl.col("_session_range"))
              .otherwise(None)
              .alias("_prev_range_raw")
        )
        .with_columns(
            pl.col("_prev_range_raw").shift(1).forward_fill().alias("prev_session_range")
        )
        .drop(["_is_last_eth_tick", "_session_range", "_prev_range_raw"])
    )


def run_step_a(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .pipe(add_prev_session_range)
        .pipe(filter_operative_session)
        .pipe(add_cvd_directional)
        .pipe(add_market_state_mr)
        .pipe(add_aggression_flag)
    )
# endregion


# region ------------------------------- STEP B ----------------------------------------
def add_location_flags(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("LVN") == 1).alias("loc_A"),
        (pl.col("ValleysPeaks").is_in([-2, -1])).alias("loc_B"),
        ((pl.col("LVN") == 1) & (pl.col("ValleysPeaks").is_in([-2, -1]))).alias("loc_C"),
    )


def build_bar_first_index(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .sort(["current_bar_datetime", "Sequence"])
        .group_by("current_bar_datetime")
        .agg(pl.col("Index").first().alias("first_index"))
    )


def filter_no_opposing_bubble(triggers: pl.DataFrame, df_full: pl.DataFrame) -> pl.DataFrame:
    """Drop bars where, after the trigger, an opposing big bubble appears (opposing_type = 3 - trigger_type)."""
    all_big = (
        df_full
        .filter(pl.col("Volume") >= BIG_ORDER_THRESHOLD)
        .select([
            "current_bar_datetime",
            pl.col("Sequence").alias("opp_seq"),
            pl.col("TradeType").alias("opp_type"),
        ])
    )
    triggers_lookup = triggers.select([
        "current_bar_datetime",
        pl.col("Sequence").alias("trigger_seq"),
        (3 - pl.col("TradeType")).cast(pl.Int64).alias("opposing_type"),
    ])
    invalidated_bars = (
        triggers_lookup
        .join(all_big, on="current_bar_datetime", how="left")
        .filter(
            (pl.col("opp_seq") > pl.col("trigger_seq"))
            & (pl.col("opp_type") == pl.col("opposing_type"))
        )
        .select("current_bar_datetime")
        .unique()
    )
    return triggers.join(invalidated_bars, on="current_bar_datetime", how="anti")


def generate_signals_mr(
    df: pl.DataFrame,
    bar_first_index: pl.DataFrame,
    location_col: str,
    target_col: str,
) -> pl.DataFrame:
    """
    Mean Reversion signal generation. Mirror of TF with TradeType inverted.

      SHORT trigger: imbalance_up_mr   + location + TradeType==1 (SELL agg) + Volume>=threshold
      LONG  trigger: imbalance_down_mr + location + TradeType==2 (BUY  agg) + Volume>=threshold

    Bar confirmation, opposing-bubble filter, entry at next_bar_open and stop
    loss rules identical to TF (mirrored by signal_direction). TP = `target_col`.
    """
    triggers = (
        df
        .filter(
            (
                (pl.col("market_state") == "imbalance_up_mr")
                & (pl.col(location_col))
                & (pl.col("TradeType") == 1)
                & (pl.col("Volume") >= BIG_ORDER_THRESHOLD)
            ) | (
                (pl.col("market_state") == "imbalance_down_mr")
                & (pl.col(location_col))
                & (pl.col("TradeType") == 2)
                & (pl.col("Volume") >= BIG_ORDER_THRESHOLD)
            )
        )
        .sort(["current_bar_datetime", "Sequence"])
        .unique(subset=["current_bar_datetime"], keep="first")
        .sort("current_bar_datetime")
    )

    triggers = filter_no_opposing_bubble(triggers, df)

    return (
        triggers
        .with_columns(
            pl.when(pl.col("market_state") == "imbalance_down_mr")
              .then(pl.lit("long"))
              .otherwise(pl.lit("short"))
              .alias("signal_direction")
        )
        .with_columns(
            pl.when(pl.col("signal_direction") == "long")
              .then(
                  (pl.col("current_bar_close") > pl.col("AskPrice"))
                  & (pl.col("current_bar_close") > pl.col("current_bar_open"))
              )
              .otherwise(
                  (pl.col("current_bar_close") < pl.col("BidPrice"))
                  & (pl.col("current_bar_close") < pl.col("current_bar_open"))
              )
              .alias("bar_confirmed")
        )
        .filter(pl.col("bar_confirmed"))
        .with_columns(pl.col("next_bar_open").alias("entry_price"))
        .join(
            bar_first_index.rename({"first_index": "entry_index"}),
            left_on="next_bar_datetime",
            right_on="current_bar_datetime",
            how="left",
        )
        .with_columns(pl.col(target_col).alias("tp_target"))
        .filter(
            pl.when(pl.col("signal_direction") == "long")
              .then(pl.col("entry_price") < pl.col("tp_target"))
              .otherwise(pl.col("entry_price") > pl.col("tp_target"))
        )
        .with_columns(
            pl.when(pl.col("signal_direction") == "long")
              .then(pl.col("AskPrice") - STOP_TICKS * TICK_SIZE)
              .otherwise(pl.col("BidPrice") + STOP_TICKS * TICK_SIZE)
              .alias("stop_loss"),
        )
        .select([
            pl.col("Index").alias("signal_index"),
            pl.col("entry_index").alias("Index"),
            "current_bar_datetime",
            "next_bar_datetime",
            "Hour",
            "signal_direction",
            "market_state",
            "TradeType",
            "entry_price",
            "stop_loss",
            "Prev_POC",
            "POC",
            "tp_target",
            "Volume",
            "CVD",
            "AskPrice",
            "BidPrice",
            "current_bar_open",
            "current_bar_high",
            "current_bar_low",
            "current_bar_close",
            "next_bar_open",
            "next_bar_high",
            "next_bar_low",
            "next_bar_close",
            "LVN",
            "ValleysPeaks",
            "prev_session_range",
        ])
    )
# endregion


# region ----------------------------- EXECUTION ---------------------------------------
def run_one_combo(
    df_with_location: pl.DataFrame,
    bar_first_index: pl.DataFrame,
    df_ticks_pd,
    month: str,
    variant_name: str,
    location_col: str,
    target_label: str,
    target_col: str,
) -> None:
    print(f"\n{'─' * 60}")
    print(f"[{month}] Variante: MR_{variant_name}_{target_label}  "
          f"(location={location_col}, target={target_col})")

    signals = generate_signals_mr(df_with_location, bar_first_index, location_col, target_col)
    signals = signals.filter(
        (pl.col("next_bar_datetime").dt.hour() < SESSION_END_HOUR)
        | (
            (pl.col("next_bar_datetime").dt.hour() == SESSION_END_HOUR)
            & (pl.col("next_bar_datetime").dt.minute() < SESSION_END_MINUTE)
        )
    ).drop_nulls(subset=["Index"])

    total = len(signals)
    if total == 0:
        print("  Nessun segnale generato.")
        return

    longs   = signals.filter(pl.col("signal_direction") == "long").height
    shorts  = signals.filter(pl.col("signal_direction") == "short").height
    avg_vol = signals["Volume"].mean()
    print(f"  Segnali: {total}  (long={longs}, short={shorts})  avg_vol={avg_vol:.1f}")

    signals_pd = (
        signals.to_pandas()
        .assign(
            TP_Ticks=lambda x: abs(x['tp_target'] - x['entry_price']) / tick_size,
            SL_Ticks=lambda x: abs(x['stop_loss'] - x['entry_price']) / tick_size,
        )
        .sort_values("Index")
        .reset_index(drop=True)
    )

    exit_strategy = CompositeExit([
        DynamicTPSLExit(signals_df=signals_pd, tick_size=tick_size),
        HourBasedExit(close_hour=SESSION_END_HOUR, close_minute=SESSION_END_MINUTE),
    ])

    result = engine.run(data=df_ticks_pd, signals=signals_pd, exit_strategy=exit_strategy)
    result.summary()

    out = (
        pl.from_pandas(result.trades_df)
        .join(
            signals.select([
                "Index", "prev_session_range", "Volume",
                "tp_target", "POC", "Prev_POC",
            ]),
            left_on="entry_timestamp",
            right_on="Index",
            how="left",
        )
    )
    fname = f"trades_MR_{variant_name}_{target_label}_{month}.parquet"
    out.write_parquet(str(OUT_DIR / fname))
    print(f"  Saved: {fname}  rows={out.height}")


def process_month(month: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  PROCESSING MONTH {month}")
    print(f"{'=' * 70}")

    parquet_path = FOLDER + f'{month}_ES.parquet'
    if not Path(parquet_path).exists():
        print(f"  [SKIP] File not found: {parquet_path}")
        return

    df_ticks_with_bars = pl.read_parquet(parquet_path, columns=COLUMNS_NEEDED)
    print(f"[{month}] Shape: {df_ticks_with_bars.shape}  "
          f"Mem: {df_ticks_with_bars.estimated_size('mb'):.1f} MB")

    df_step_a = run_step_a(df_ticks_with_bars)

    print(f"[{month}] Market State Distribution (MR):")
    print(
        df_step_a.group_by("market_state").agg(pl.len().alias("count"))
        .with_columns((pl.col("count") / pl.col("count").sum() * 100).round(2).alias("pct"))
        .sort("count", descending=True)
    )

    df_with_location = add_location_flags(df_step_a)
    bar_first_index  = build_bar_first_index(df_with_location)

    df_ticks_pd = df_ticks_with_bars.select(
        ['Date', 'Datetime', 'Index', 'Price', 'SessionType', 'Time']
    ).to_pandas()

    for v_name, (loc_col, _) in LOCATION_VARIANTS.items():
        for t_label, t_col in TARGET_VARIANTS.items():
            run_one_combo(
                df_with_location, bar_first_index, df_ticks_pd,
                month, v_name, loc_col, t_label, t_col,
            )

    # Memory hygiene — Windows 32GB box; explicit del + gc per CLAUDE.md
    del df_ticks_with_bars, df_step_a, df_with_location, bar_first_index, df_ticks_pd
    gc.collect()
    print(f"[{month}] Memory released.")


OUT_DIR.mkdir(parents=True, exist_ok=True)
print(f"Output dir: {OUT_DIR}")
print(f"Months to process: {MONTHS}")

for month in MONTHS:
    process_month(month)

print(f"\n{'=' * 70}")
print("All months processed.")
print(f"{'=' * 70}")
# endregion
