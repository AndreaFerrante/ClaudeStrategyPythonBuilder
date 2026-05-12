from dataclasses import dataclass, field
from typing import Any, Dict
import math
from datetime import time

import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

import orderflow.configuration as cf

from orderflow._volume_factory import get_tickers_in_folder_mem_optim, get_market_evening_session
from orderflow.volume_profile import get_volume_profile_peaks_valleys, get_daily_high_and_low_by_session, get_dynamic_cumulative_delta_per_session
from orderflow.volume_profile_kde import gaussian_kde_numba_parallel, get_kde_high_low_price_peaks
from orderflow.volume_profile import get_volume_profile_areas, get_volume_profile_node_volume, get_daily_session_moving_POC
from orderflow.auctions import (
    aggregate_auctions,
    get_valid_blocks,
    compute_forward_outcomes,
)

from orderflow.compressor import compress_to_minute_bars_pl

from orderflow.backtester.execution import SlippageMode, SlippageModel
from orderflow.backtester.engine import BacktestEngine
from orderflow.backtester.models import ExitSignal, ExitReason, Side, Tick, PositionState
from orderflow.backtester.exits import BaseExitStrategy, CompositeExit, FixedTPSLExit, TrailingStopExit


# See 100 elements in polars tables while printing !
pl.Config.set_tbl_rows(100)


#region ------------------------------- CONFIG ----------------------------------------
FOLDER         = r"C:/Users/ZH-APPLICATION/Documents/Pycharm/Orderflow/sources/ES/"
TICKER         = "ES"
MARKET         = "CME"
SEPARATOR      = ";"
EXTENSION      = ".txt"

# Strategy params
N_CONSECUTIVE  = 2
VOL_THRESH     = 100
MINUTES_AHEAD  = 3
MIN_ABS_IMB    = 1.5 # <-- NEW: require |Imbalance| >= 2.0 (ratio mode => ≥2x dominance)

tick_size = cf.FUTURE_VALUES.loc[cf.FUTURE_VALUES['Ticker'] == TICKER, 'Tick_Size'].values[0]
tick_value = cf.FUTURE_VALUES.loc[cf.FUTURE_VALUES['Ticker'] == TICKER, 'Tick_Value'].values[0]
#endregion

# ----------------- LOAD TICKS FOR THE MONTH (CONCAT WEEKS) -------------------
# this method uses less memory, useful when more than one file needed to be loaded
df_ticks = get_tickers_in_folder_mem_optim(
    path      = FOLDER,
    ticker    = TICKER,
    extension = EXTENSION,
    separator = SEPARATOR,
    market    = MARKET
)

#region  ------------------------------- ENRICH TICKS 1 --------------------------------
df_ticks = df_ticks.with_columns(Hour = pl.col("Datetime").dt.hour())
df_ticks = df_ticks.with_columns(SessionType = get_market_evening_session(data=df_ticks, ticker="ES"))

# CHECKPOINT 1: save intermediate file -----------------------------------------
# df_ticks.write_parquet(FOLDER + 'checkpoint_1.parquet', compression="snappy")
# ------------------------------------------------------------------------------
# CHECKPOINT 1: load intermediate file -----------------------------------------
# df_ticks_lazy = pl.scan_parquet(FOLDER + 'checkpoint_1.parquet')
# df_ticks = df_ticks_lazy.collect()
# ------------------------------------------------------------------------------
cols_needed = ['Datetime', 'Price', 'Volume', 'SessionType']
df_ticks_pd = df_ticks.select(cols_needed).to_pandas() # converto to pandas only a subset of the df
df_ticks_pd['Datetime'] = pd.Series(
    df_ticks_pd['Datetime'].dt.to_pydatetime(),
    dtype=object    # ← forza pandas a tenere Python datetime puri
)
POC, PREV_POC = get_daily_session_moving_POC(df_ticks_pd)
df_ticks = df_ticks.with_columns([
    pl.Series("POC", POC),
    pl.Series("Prev_POC", PREV_POC),
])

# CHECKPOINT 2: save intermediate file -----------------------------------------
# df_ticks.write_parquet(FOLDER + 'checkpoint_2.parquet', compression="snappy")
# ------------------------------------------------------------------------------
# CHECKPOINT 2: load intermediate file -----------------------------------------
# df_ticks_lazy = pl.scan_parquet(FOLDER + 'checkpoint_2.parquet')
# df_ticks = df_ticks_lazy.collect()
# ------------------------------------------------------------------------------
cols_needed = ['Price', 'Volume', 'SessionType']
df_ticks_pd = df_ticks.select(cols_needed).to_pandas() # converto to pandas only a subset of the df
df_ticks = df_ticks.with_columns(VA_Areas     = get_volume_profile_areas(df_ticks_pd))
df_ticks = df_ticks.with_columns(ValleysPeaks = get_volume_profile_peaks_valleys(df_ticks_pd))

# CHECKPOINT 3: save intermediate file -----------------------------------------
# df_ticks.write_parquet(FOLDER + 'checkpoint_3.parquet', compression="snappy")
# CHECKPOINT 3: load intermediate file -----------------------------------------
# df_ticks_lazy = pl.scan_parquet(FOLDER + 'checkpoint_3.parquet')
# df_ticks = df_ticks_lazy.collect()
# ------------------------------------------------------------------------------
cols_needed = ['Price', 'Volume', 'TradeType', 'SessionType']
df_ticks_pd = df_ticks.select(cols_needed).to_pandas() # converto to pandas only a subset of the df
df_cd = get_dynamic_cumulative_delta_per_session(df_ticks_pd)
df_ticks = df_ticks.with_columns([
    pl.Series("CD_Ask",   df_cd["CD_Ask"].values),
    pl.Series("CD_Bid",   df_cd["CD_Bid"].values),
    pl.Series("CD_Total", df_cd["CD_Total"].values),
])
lows, highs  = get_daily_high_and_low_by_session(df_ticks_pd)
df_ticks = df_ticks.with_columns([
    pl.Series("Session_High", highs),
    pl.Series("Session_Low", lows),
])
price_tot_volume, price_askvolume, price_bidvolume, total_volumes = get_volume_profile_node_volume(df_ticks_pd)
df_ticks = df_ticks.with_columns([
    pl.Series("Node_Volume", price_tot_volume),
    pl.Series("Node_Ask_Volume", price_askvolume),
    pl.Series("Node_Bid_Volume", price_bidvolume),
    pl.Series("Session_Volume", total_volumes),
])
# Identify LVN by Volume distibution
df_ticks = df_ticks.with_columns(
    (
        pl.col("Node_Volume")
        < 0.25
        * (pl.col("Session_Volume") / ((pl.col("Session_High") - pl.col("Session_Low")) / tick_size))
    )
    .cast(pl.Int8)
    .alias("LVN")
)
# needed to make the back tester engine work with polars df (add Index column)
df_ticks = df_ticks.with_columns(
    Index=pl.int_range(0, pl.len()).alias("Index")
)

# CHECKPOINT 4: save intermediate file -----------------------------------------
# df_ticks.write_parquet(FOLDER + 'checkpoint_4.parquet', compression="snappy")
# ------------------------------------------------------------------------------
# df_ticks_lazy = pl.scan_parquet(FOLDER + 'checkpoint_4.parquet')
# df_ticks = df_ticks_lazy.collect()
# CHECKPOINT 4: load intermediate file -----------------------------------------
# ------------------------------------------------------------------------------
#endregion

#region  ------------------------------- ENRICH TICKS 2 --------------------------------
# add 1 minute bars info
one_min_df_bars = compress_to_minute_bars_pl(df_ticks,
                                             win_compression = '1m',
                                             time_column="Datetime")
one_min_df_bars = one_min_df_bars.with_columns(
    pl.col("Datetime").dt.replace_time_zone(None)  # ← Remove timezone
)
one_min_df_bars = one_min_df_bars.with_columns([
        pl.col("Datetime").alias("current_bar_datetime")
    ])
rename_map = {
        col: f"current_bar_{col.lower()}" 
        for col in one_min_df_bars.columns
        if col not in ["Datetime", "current_bar_datetime"]
    }

one_min_df_bars_renamed = one_min_df_bars.rename(rename_map)

one_min_df_bars_extended = one_min_df_bars_renamed.with_columns([
        # Datetime next bar
        pl.col("Datetime").shift(-1).alias("next_bar_datetime"),
        
        # next bar OHLCV
        pl.col("current_bar_open").shift(-1).alias("next_bar_open"),
        pl.col("current_bar_high").shift(-1).alias("next_bar_high"),
        pl.col("current_bar_low").shift(-1).alias("next_bar_low"),
        pl.col("current_bar_close").shift(-1).alias("next_bar_close"),
        pl.col("current_bar_volume").shift(-1).alias("next_bar_volume"),
        pl.col("current_bar_numberoftrades").shift(-1).alias("next_bar_num_trades"),
        pl.col("current_bar_askvolume").shift(-1).alias("next_bar_ask_volume"),
        pl.col("current_bar_bidvolume").shift(-1).alias("next_bar_bid_volume"),
    ])
# join bars to ticks
df_ticks_with_bars = df_ticks.join_asof(
        one_min_df_bars_extended,
        left_on="Datetime",
        right_on="Datetime",
        strategy="backward"
    )

# CHECKPOINT 5: save intermediate file -----------------------------------------
# df_ticks_with_bars.write_parquet(FOLDER + 'checkpoint_5.parquet', compression="snappy")
# ------------------------------------------------------------------------------
#endregion-------

#region ----------------------  Plotting the KDE curve overlap here -------------------
# df_ticks_gb   = (df_ticks.
#                  group_by("Price").
#                  agg(pl.sum("Volume"),
#                      pl.min("Datetime").alias("MinDatetime"),
#                      pl.max("Datetime").alias("MaxDatetime")).
#                  sort(["Price"]))
# bigger        = df_ticks.filter(pl.col('Volume') >= 50)
# bigger        = bigger.group_by(pl.col('Price')).agg(pl.sum('Volume')).sort('Price')
# prices        = np.array(df_ticks_gb['Price'])
# volumes       = np.array(df_ticks_gb['Volume'])
# min_dt        = np.array(df_ticks_gb['MinDatetime'])
# max_dt        = np.array(df_ticks_gb['MaxDatetime'])
# kde_values    = gaussian_kde_numba_parallel(source=prices, weight=volumes, h=.5)
# kde_peaks     = get_kde_high_low_price_peaks(kde_values)
# pv_prices     = prices[kde_peaks]
# kde_df        = pd.DataFrame({'Price':prices, 'Volume':volumes ,'kde':kde_values})

# fig, ax1 = plt.subplots()
# ax1.set_xlabel('Price')
# ax1.set_ylabel('Volume / Counter', color='red')
# ax1.plot(kde_df['Price'], kde_df['kde'], color='red')
# ax2 = ax1.twinx()
# ax2.bar(prices, volumes, color='blue', edgecolor='black', alpha=0.5, width=0.25)
# ax3 = ax2.twinx()
# ax3.scatter(pv_prices, kde_values[kde_peaks], color='lime', zorder=5)
# fig.tight_layout()
# plt.show()
#endregion

#region ----------- CALCULATE AUCTIONS (ratio mode so 2.0 means ≥2x dominance) --------
df_agg = aggregate_auctions(
    df             = df_ticks,
    imbalance_mode = "ratio",  # <-- important for MIN_ABS_IMB=2.0
).with_columns(
    pl.when(pl.col("BuyVolume") > pl.col("SellVolume"))
      .then(pl.lit("Long"))
      .otherwise(pl.lit("Short"))
    .alias("TradeSide")
)
print("[Monthly] Auctions:", df_agg.shape)


# ----------------- BLOCKS (EVENTS) with |Imbalance| >= 2.0 -------------------
df_blocks = get_valid_blocks(
    agg               = df_agg,
    n_consecutive     = N_CONSECUTIVE,
    vol_thresh        = VOL_THRESH,
    min_abs_imbalance = MIN_ABS_IMB,   # <-- require strong imbalance for entries
)
print("[Monthly] Blocks:", df_blocks.shape)
#endregion

#region ------------------ Add blocks info to the TRADE TRIGGERS ----------------------
# A score is calcuated to measure if and when a block of consecutive unbalance
# auctions happened before the trigger.
# score results:
# 1.0: blocks within 1 minute before
# 0.7: blocks between 1 and 2 minutes before
# 0.4: blocks between 2 and 3 minutes before
# 0.0: otherwise

# Step 1: add direction info column
df_blocks_prepared = df_blocks.with_columns([
    # 1 (bullish), -1 (bearish), 0 (otherwise)
    pl.when(pl.col("TotalBlockImbalance") > 0)
    .then(pl.lit(1))
    .when(pl.col("TotalBlockImbalance") < 0)
    .then(pl.lit(-1))
    .otherwise(pl.lit(0))
    .alias("block_direction")
])
# Step 2: add direction info column
df_ticks_with_bars_prepared = df_ticks_with_bars.with_columns([
    # 1 (buy) o -1 (sell)
    pl.when(pl.col("TradeType") == 2)
    .then(pl.lit(1))
    .when(pl.col("TradeType") == 1)
    .then(pl.lit(-1))
    .otherwise(pl.lit(0))
    .alias("tick_direction")
])
# Step 3: join dfs
df_ticks_with_bars_blocks = df_ticks_with_bars_prepared.join_asof(
        df_blocks_prepared,
        left_on="Datetime",
        right_on="EndTime",
        strategy="backward",
        suffix="_block"
    )
# Step 4: Calculate time distance and score
df_ticks_with_bars_blocks = df_ticks_with_bars_blocks.with_columns([
    # Distancd in secs
    (pl.col("Datetime") - pl.col("EndTime"))
    .dt.total_seconds()
    .alias("block_distance_sec"),
])
# Step 5: Filter by lookback window and same direction
df_ticks_with_bars_blocks = df_ticks_with_bars_blocks.with_columns([
    # Block is valid if:
    # 1. Exists (EndTime not null)
    # 2. it is inside the lookback window
    # 3. same tick direction
    pl.when(
        pl.col("EndTime").is_not_null() &
        (pl.col("block_distance_sec") <= 3 * 60) & # 3 minutes max
        (pl.col("block_direction") == pl.col("tick_direction")) &
        (pl.col("block_direction") != 0)
    )
    .then(pl.lit(True))
    .otherwise(pl.lit(False))
    .alias("has_aligned_block")
])
# Step 6: Calculate proximity score
df_ticks_with_bars_blocks = df_ticks_with_bars_blocks.with_columns([
    pl.when(~pl.col("has_aligned_block"))
    .then(pl.lit(0.0))
    .when(pl.col("block_distance_sec") <= 60)  # 0-1 minute
    .then(pl.lit(1.0))
    .when(pl.col("block_distance_sec") <= 120)  # 1-2 minutes
    .then(pl.lit(0.7))
    .when(pl.col("block_distance_sec") <= 180)  # 2-3 minutes
    .then(pl.lit(0.4))
    .otherwise(pl.lit(0.0))
    .alias("block_score")
])
 # Step 7: clean 1
df_ticks_with_bars_blocks = df_ticks_with_bars_blocks.with_columns([
    pl.when(pl.col("has_aligned_block"))
    .then(pl.col("TotalBlockImbalance"))
    .otherwise(pl.lit(None))
    .alias("block_imbalance"),
    
    
    pl.when(pl.col("has_aligned_block"))
    .then(pl.col("block_distance_sec"))
    .otherwise(pl.lit(None))
    .alias("block_distance_sec_final")
])
# Step 8: clean 2
cols_to_drop = [
    "tick_direction",
    "block_direction", 
    "AuctionStartId_block",
    "AuctionEndId_block",
    "EndTime",
    "StartTime",
    "TotalBlockVolume",
    "TotalBlockImbalance",
    "BlockId_block",
    "block_distance_sec",
    "has_aligned_block",
    "block_imbalance",
    "bloc_distance_sec"
]   

existing_cols_to_drop = [c for c in cols_to_drop if c in df_ticks_with_bars_blocks.columns]
df_ticks_with_bars_blocks = df_ticks_with_bars_blocks.drop(existing_cols_to_drop)

if "block_distance_sec_final" in df_ticks_with_bars_blocks.columns:
    df_ticks_with_bars_blocks = df_ticks_with_bars_blocks.rename({"block_distance_sec_final": "block_distance_sec"})
#endregion

#region ----------- FIND TRADE TRIGGERS TICKS (FIRST TRY) (REQUIRES BLOCKS) -----------
# filter by time
ticks_signals = df_ticks_with_bars_blocks.filter(
    pl.col("Datetime").dt.time().is_between(
        time(8, 30, 0), 
        time(15, 0, 0),
        closed="both"
    )
)

# identify big prints
ticks_signals = ticks_signals.filter(
    pl.col('Volume') >= 50
)

# Condition 1: TradeType=2 (buy), outside VA, > POC, is LVN or Low Volume Area
buy_signal = (
    (pl.col('TradeType') == 2) &
    (pl.col('VA_Areas') == 'na') &
    (pl.col('Price') > pl.col('POC')) &
    (
        #(pl.col('LVN') == 1) | 
     (pl.col('ValleysPeaks') <= -1))
)
# Condition 2: TradeType=1 (sell), outside VA, < POC, is LVN or Low Volume Area
sell_signal = (
    (pl.col('TradeType') == 1) &
    (pl.col('VA_Areas') == 'na') &
    (pl.col('Price') < pl.col('POC')) &
    (
       #(pl.col('LVN') == 1) |
    (pl.col('ValleysPeaks') <= -1))
)

# buy_signal OR sell_signal
ticks_signals = ticks_signals.filter(buy_signal | sell_signal)
#endregion

#region ---------------------------------- BACK TEST ----------------------------------

engine = BacktestEngine(
        tick_size=tick_size,
        tick_value=tick_value,
        commission=0.9,
        n_contracts=1,
        slippage_model=SlippageModel(mode=SlippageMode.UNIFORM, max_ticks=2, seed=42),
        progress_bar=False,
    )

df_ticks_pd = df_ticks.to_pandas()

ticks_signals_pd = ticks_signals.to_pandas()

result = engine.run(
    data=df_ticks_pd,
    signals=ticks_signals_pd,
    tp_ticks=8,
    sl_ticks=4,
)

result.summary()
print(f"\nTrades DataFrame shape: {result.trades_df.shape}")
print(result.trades_df.head(5).to_string(index=False))


ticks_signals_pd_2 = ticks_signals_pd.query("block_score > 0").copy()

result_2 = engine.run(
    data=df_ticks_pd,
    signals=ticks_signals_pd_2,
    tp_ticks=12,
    sl_ticks=4,
)

result_2.summary()
print(f"\nTrades DataFrame shape: {result_2.trades_df.shape}")
print(result_2.trades_df.head(5).to_string(index=False))


exit_logic_2_2 = CompositeExit([
        FixedTPSLExit(tp=12, sl=4, tick_size=0.25),
        TrailingStopExit(trail_ticks=5, tick_size=0.25, activation_ticks=1)
    ])

result_2_2 = engine.run(
    data=df_ticks_pd,
    signals=ticks_signals_pd_2,
    exit_strategy=exit_logic_2_2
)

result_2_2.summary()
print(f"\nTrades DataFrame shape: {result_2_2.trades_df.shape}")
print(result_2_2.trades_df.head(5).to_string(index=False))

  
ticks_signals_pd_3 = ticks_signals_pd.query("LVN > 0").copy()

result_3 = engine.run(
    data=df_ticks_pd,
    signals=ticks_signals_pd_3,
    tp_ticks=8,
    sl_ticks=4,
)

result_3.summary()
print(f"\nTrades DataFrame shape: {result_3.trades_df.shape}")
print(result_3.trades_df.head(5).to_string(index=False))


ticks_signals_pd_4 = ticks_signals_pd.query(
    '(TradeType == 2 and Node_Ask_Volume > Node_Bid_Volume) or \
     (TradeType == 1 and Node_Bid_Volume > Node_Ask_Volume)').copy()

result_4 = engine.run(
    data=df_ticks_pd,
    signals=ticks_signals_pd_4,
    tp_ticks=12,
    sl_ticks=4,
)

result_4.summary()
print(f"\nTrades DataFrame shape: {result_4.trades_df.shape}")
print(result_4.trades_df.head(5).to_string(index=False))

#endregion

#region ------------------------------- FRANCESCO ? -----------------------------------
df_blocks = df_blocks.join_asof(df_ticks.select(['Datetime', 'Price']),
                                left_on  = "StartTime",
                                right_on = "Datetime",
                                strategy = "backward")
df_blocks = df_blocks.join_asof(df_agg.select(['StartTime', 'TradeSide']),
                                left_on  = "StartTime",
                                right_on = "StartTime",
                                strategy = "backward")

longs  = df_blocks.filter(df_blocks['TradeSide']=='Long')
shorts = df_blocks.filter(df_blocks['TradeSide']=='Short')
plt.plot(df_ticks['Datetime'], df_ticks['Price'])
plt.scatter(shorts['EndTime'], shorts['Price'], color='red', zorder=5)
plt.scatter(longs['EndTime'], longs['Price'], color='lime', zorder=5)
# --------------------------------------------------------------------

if df_blocks.is_empty():
    print(f"No blocks.")
else:
    # ------------- FORWARD OUTCOMES (MICRO-BACKTEST) -------------------------
    df_fwd = compute_forward_outcomes(
        df_ticks      = df_ticks,
        blocks        = df_blocks,
        minutes_ahead = MINUTES_AHEAD
    ).join(
        df_agg.select(["AuctionId","TradeSide"]),
        left_on="AuctionEndId", right_on="AuctionId", how="left"
    ).with_columns(
        pl.when(pl.col("TradeSide")=="Short")
          .then(-pl.col("SimpleReturnInTicks"))
          .otherwise(pl.col("SimpleReturnInTicks"))
          .alias("PnLTicks")
    )

    # ------------- QUICK STATS + CUM PnL PLOT --------------------------------
    pnl = df_fwd.get_column("PnLTicks").to_numpy() if "PnLTicks" in df_fwd.columns else np.array([])
    if pnl.size:
        hits  = (pnl > 0).mean()
        avg   = pnl.mean()
        std   = pnl.std(ddof=1) if pnl.size > 1 else float("nan")
        tstat = avg / (std / math.sqrt(pnl.size)) if (pnl.size > 1 and std > 0) else float("nan")
        cum   = np.cumsum(pnl)

        # max drawdown
        peak, max_dd = -1e18, 0.0
        for v in cum:
            peak   = max(peak, v)
            max_dd = max(max_dd, peak - v)

        print(f"Trades: {pnl.size} | Hit: {hits:.1%} | Avg: {avg:.3f} ticks | "f"T: {tstat:.3f} | MaxDD: {max_dd:.1f}")

        plt.figure()
        plt.plot(cum)
        plt.title("Cumulative PnL (ticks)")
        plt.xlabel("Event index")
        plt.ylabel("Cum PnL (ticks)")
        plt.title(f"Ticker {TICKER}, Consecutive {N_CONSECUTIVE}, VolThres {VOL_THRESH}, MinImb {MIN_ABS_IMB}, Ahead {MINUTES_AHEAD}min")
        plt.tight_layout()
        plt.show()

    else:
        print(f"No PnLTicks computed — check joins/columns.")
#endregion

#region ----------------- FIND STRATEGY WITH CLAUDE (NO BLOCKS) -----------------------
SESSION_START_HOUR = 8
SESSION_START_MINUTE = 45
SESSION_END_HOUR = 15
SESSION_END_MINUTE = 0

# Filtro contratti per "Big Bubble" (aggression trigger)
# Fabio dice 30 contratti su NQ 1-min → ES è meno volatile,
# 20 è un punto di partenza ragionevole da validare sui dati
BIG_ORDER_THRESHOLD = 20

COLUMNS_NEEDED = [
    # ── Identificazione e ordinamento ────────────────────────
    "Index",                    # riferimento backtest engine
    "Sequence",                 # ordinamento naturale tick dentro la barra
    "Datetime",                 # timestamp tick

    # ── Filtro sessione ───────────────────────────────────────
    "Hour",
    "SessionType",
    "Prev_POC",                 # filtro Prev_POC > 0 + target trade

    # ── Tick trigger ─────────────────────────────────────────
    "TradeType",                # 1=buy, 2=sell aggression
    "Volume",                   # soglia big bubble
    "AskPrice",                 # prezzo trigger long + conferma barra
    "BidPrice",                 # prezzo trigger short + conferma barra
    "Price",                    # per market_state (confronto con POC)

    # ── Market State ──────────────────────────────────────────
    "VA_Areas",                 # balance vs imbalance
    "POC",                      # POC corrente sessione ETH
    "LVN",                      # location flag A
    "ValleysPeaks",             # location flag B

    # ── CVD tre livelli ───────────────────────────────────────
    "CD_Ask",                   # → CVD sessione ETH
    "CD_Bid",
    "current_bar_askvolume",    # → current_bar_cvd
    "current_bar_bidvolume",
    "Node_Ask_Volume",          # → node_cvd
    "Node_Bid_Volume",

    # ── Barra 1-min corrente (trigger + stop loss) ────────────
    "current_bar_datetime",
    "current_bar_open",
    "current_bar_high",
    "current_bar_low",
    "current_bar_close",

    # ── Barra 1-min successiva (entry) ────────────────────────
    "next_bar_datetime",
    "next_bar_open",
    "next_bar_high",
    "next_bar_low",
    "next_bar_close",
]

# Carica solo le colonne necessarie dal parquet
df_ticks_with_bars = pl.read_parquet(
    FOLDER + 'checkpoint_5.parquet',
    columns=COLUMNS_NEEDED,
)

print(f"Shape: {df_ticks_with_bars.shape}")
print(f"Memoria stimata: {df_ticks_with_bars.estimated_size('mb'):.1f} MB")

# ─────────────────────────────────────────────
# 2. FILTRO SESSIONE RTH OPERATIVA
# ─────────────────────────────────────────────

def filter_operative_session(df: pl.DataFrame) -> pl.DataFrame:
    """
    Filtra i tick nella finestra operativa NY:
    - SessionType == RTH
    - Datetime tra 08:45 e 15:00 CT
    - Esclude prima sessione dove Prev_POC == 0 (nessun riferimento storico)
    """
    return (
        df
        .filter(pl.col("SessionType") == "RTH")
        .filter(pl.col("Prev_POC") > 0)
        .with_columns(
            # Estrae minuti per filtro preciso intra-ora
            pl.col("Datetime").dt.minute().alias("_minute")
        )
        .filter(
            # Dopo 08:45
            (
                (pl.col("Hour") == SESSION_START_HOUR) &
                (pl.col("_minute") >= SESSION_START_MINUTE)
            ) |
            (pl.col("Hour") > SESSION_START_HOUR)
        )
        .filter(
            # Prima di 15:00 (escluso)
            pl.col("Hour") < SESSION_END_HOUR
        )
        .drop("_minute")
    )


# ─────────────────────────────────────────────
# 3. CVD DIREZIONALE
# ─────────────────────────────────────────────

def add_cvd_directional(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tre livelli di CVD con granularità crescente:

    1. CVD (sessione ETH) = CD_Bid - CD_Ask
       Pressione direzionale accumulata dall'inizio della sessione.
       Fabio lo usa per anticipare il break-even e leggere divergenze
       tra prezzo e pressione cumulativa.

    2. current_bar_cvd (barra 1-min) = current_bar_bidvolume - current_bar_askvolume
       Chi ha dominato aggressivamente nell'ultima barra 1-min.
       Conferma di breve termine sul tick trigger.

    3. node_cvd (nodo VP) = Node_Bid_Volume - Node_Ask_Volume
       Chi ha storicamente dominato su quel preciso livello di prezzo del VP.
       Fabio: "you can see for each level who is dominating the market"
       Segnale più affidabile quando allineato con la direzione del trade.

    Convenzione comune: positivo → buy dominante, negativo → sell dominante.
    """
    return df.with_columns(
        # 1. CVD cumulativo sessione ETH
        (pl.col("CD_Bid") - pl.col("CD_Ask"))
        .alias("CVD"),

        # 2. CVD barra 1-min corrente
        (pl.col("current_bar_bidvolume") - pl.col("current_bar_askvolume"))
        .alias("current_bar_cvd"),

        # 3. CVD nodo Volume Profile
        (pl.col("Node_Bid_Volume") - pl.col("Node_Ask_Volume"))
        .alias("node_cvd"),

        # Delta istantaneo tick
        pl.when(pl.col("TradeType") == 1)
          .then(pl.col("Volume"))
          .when(pl.col("TradeType") == 2)
          .then(-pl.col("Volume"))
          .otherwise(0)
          .alias("tick_delta"),
    )

# ─────────────────────────────────────────────
# 4. MARKET STATE CLASSIFICATION
# ─────────────────────────────────────────────

def add_market_state(df: pl.DataFrame) -> pl.DataFrame:
    """
    Market State per ogni tick basato su VA_Areas e ValleysPeaks.

    Logica:
    - BALANCE:
        VA_Areas in ('VA', 'PO') → siamo dentro la value area o sul POC
        → mercato in equilibrio, modello trend following NON attivo

    - IMBALANCE_UP:
        VA_Areas == 'na' AND price > POC
        → prezzo sopra la value area, sellers deboli, breakout up
        → valida solo se Prev_POC > current price (target raggiungibile)

    - IMBALANCE_DOWN:
        VA_Areas == 'na' AND price < POC
        → prezzo sotto la value area, buyers deboli, breakout down
        → valida solo se Prev_POC < current price (target raggiungibile)

    ValleysPeaks usato come filtro di qualità:
    - ValleysPeaks in (-2, -1) → siamo su una valle del profilo
      (LVN zona) → imbalance più affidabile
    - ValleysPeaks in (1, 2) → siamo su un picco (HVN) → 
      più resistenza, imbalance meno affidabile
    """
    return df.with_columns(
        pl.when(
            (pl.col("VA_Areas").is_in(["VA", "PO"]))
        )
        .then(pl.lit("balance"))

        .when(
            (pl.col("VA_Areas") == "na") &
            (pl.col("Price") > pl.col("POC")) &
            (pl.col("Prev_POC") > pl.col("Price"))  # target sopra: valido per long
        )
        .then(pl.lit("imbalance_up"))

        .when(
            (pl.col("VA_Areas") == "na") &
            (pl.col("Price") < pl.col("POC")) &
            (pl.col("Prev_POC") < pl.col("Price"))  # target sotto: valido per short
        )
        .then(pl.lit("imbalance_down"))

        .otherwise(pl.lit("imbalance_no_target"))  # imbalance ma POC prev non allineato
        .alias("market_state")
    )


# ─────────────────────────────────────────────
# 5. FLAG AGGRESSIONE (Big Bubble)
# ─────────────────────────────────────────────

def add_aggression_flag(df: pl.DataFrame, threshold: int = BIG_ORDER_THRESHOLD) -> pl.DataFrame:
    """
    Identifica i tick con aggressione significativa (big orders).
    Fabio: 30 contratti su NQ 1-min, 20 su London.
    Su ES iniziamo con 20, da ottimizzare empiricamente.

    big_ask: buy aggression (long trigger)
    big_bid: sell aggression (short trigger)
    """
    return df.with_columns(
        (
            (pl.col("TradeType") == 1) &
            (pl.col("Volume") >= threshold)
        ).alias("big_ask"),

        (
            (pl.col("TradeType") == 2) &
            (pl.col("Volume") >= threshold)
        ).alias("big_bid"),
    )


# ─────────────────────────────────────────────
# 6. PIPELINE COMPLETA STEP A
# ─────────────────────────────────────────────

def run_step_a(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .pipe(filter_operative_session)
        .pipe(add_cvd_directional)
        .pipe(add_market_state)
        .pipe(add_aggression_flag)
    )


# ─────────────────────────────────────────────
# 7. ESPLORAZIONE DISTRIBUZIONI
# ─────────────────────────────────────────────

def explore(df: pl.DataFrame) -> None:
    print("=" * 60)
    print("STEP A — ESPLORAZIONE DATI")
    print("=" * 60)

    print(f"\nTick totali dopo filtro sessione: {len(df):,}")

    # Distribuzione Market State
    print("\n--- Market State Distribution ---")
    print(
        df.group_by("market_state")
          .agg(pl.len().alias("count"))
          .with_columns(
              (pl.col("count") / pl.col("count").sum() * 100)
              .round(2)
              .alias("pct")
          )
          .sort("count", descending=True)
    )

    # Distribuzione Volume per TradeType
    print("\n--- Volume Distribution per TradeType ---")
    print(
        df.group_by("TradeType")
          .agg(
              pl.len().alias("n_ticks"),
              pl.col("Volume").mean().round(2).alias("avg_volume"),
              pl.col("Volume").median().alias("median_volume"),
              pl.col("Volume").quantile(0.95).round(2).alias("p95_volume"),
              pl.col("Volume").max().alias("max_volume"),
          )
          .sort("TradeType")
    )

    # Quanti big orders ci sono per soglia
    print("\n--- Big Orders count per soglia Volume ---")
    for threshold in [10, 20, 30, 50, 100]:
        n = df.filter(pl.col("Volume") >= threshold).height
        pct = n / len(df) * 100
        print(f"  >= {threshold:>3} contratti: {n:>6,} tick ({pct:.3f}%)")

    # LVN distribution
    print("\n--- LVN Flag Distribution ---")
    print(
        df.group_by("LVN")
          .agg(pl.len().alias("count"))
          .with_columns(
              (pl.col("count") / pl.col("count").sum() * 100)
              .round(2)
              .alias("pct")
          )
    )

    # Segnali potenziali trend following (pre-trigger)
    potential_long = df.filter(
        (pl.col("market_state") == "imbalance_up") &
        (pl.col("LVN") == 1)
    ).height

    potential_short = df.filter(
        (pl.col("market_state") == "imbalance_down") &
        (pl.col("LVN") == 1)
    ).height

    print(f"\n--- Setup Potenziali (imbalance + LVN, pre-trigger) ---")
    print(f"  Long setups:  {potential_long:,} tick")
    print(f"  Short setups: {potential_short:,} tick")

    # CVD range
    print("\n--- CVD Statistics ---")
    print(
        df.select(
            pl.col("CVD").min().alias("CVD_min"),
            pl.col("CVD").max().alias("CVD_max"),
            pl.col("CVD").mean().round(2).alias("CVD_mean"),
            pl.col("CVD").std().round(2).alias("CVD_std"),
        )
    )

    print("\n" + "=" * 60)
    print("Step A completato. Pronto per Step B (signal generation).")
    print("=" * 60)


# ─────────────────────────────────────────────
# 8. ESECUZIONE
# ─────────────────────────────────────────────

# Assumendo che df_ticks_with_bars_prepared sia già in memoria
df_step_a = run_step_a(df_ticks_with_bars)
explore(df_step_a)


"""
Step B — Signal Generation (versione finale)
Modello Trend Following NY Session (Fabio Valentino)
Ticker: ES Futures

Varianti:
  A_LVN:    location = LVN solo
  B_valley: location = ValleysPeaks (-2,-1) solo

Filtro CVD nodo:
  La direzione della big bubble deve essere coerente con node_cvd,
  ovvero chi ha storicamente dominato quel livello di prezzo del VP.
  Fabio: "you can see for each level who is dominating the market"
  - Long:  node_cvd > 0  (buyers dominano il nodo)
  - Short: node_cvd < 0  (sellers dominano il nodo)

Prerequisito: df_step_a da Step A (include CVD, current_bar_cvd, node_cvd)
"""


# ─────────────────────────────────────────────
# COSTANTI
# ─────────────────────────────────────────────

BIG_ORDER_THRESHOLD = 10   # contratti minimi per "big bubble" su ES
STOP_TICKS = 2             # tick oltre il high/low della barra trigger
TICK_SIZE = 0.25           # ES tick size

LOCATION_VARIANTS = {
    "A_LVN":                ("loc_A", "LVN solo"),
    "B_valley":             ("loc_B", "ValleysPeaks (-2,-1) solo"),
    "B_valley_no_nodecvd":  ("loc_B", "ValleysPeaks (-2,-1) senza filtro node_cvd"),
}


# ─────────────────────────────────────────────
# 1. LOCATION FLAGS
# ─────────────────────────────────────────────

def add_location_flags(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("LVN") == 1)
        .alias("loc_A"),

        (pl.col("ValleysPeaks").is_in([-2, -1]))
        .alias("loc_B"),
    )


# ─────────────────────────────────────────────
# 2. PRE-CALCOLO INDEX PRIMO TICK PER BARRA
# ─────────────────────────────────────────────

def build_bar_first_index(df: pl.DataFrame) -> pl.DataFrame:
    """
    Per ogni barra 1-minuto restituisce l'Index del primo tick.
    Usato in join per ottenere l'entry_index (primo tick barra successiva).
    """
    return (
        df
        .sort(["current_bar_datetime", "Sequence"])
        .group_by("current_bar_datetime")
        .agg(pl.col("Index").first().alias("first_index"))
    )


# ─────────────────────────────────────────────
# 3. PIPELINE SEGNALI PER UNA VARIANTE
# ─────────────────────────────────────────────

def generate_signals(
    df: pl.DataFrame,
    bar_first_index: pl.DataFrame,
    location_col: str,
    use_node_cvd: bool = True,
) -> pl.DataFrame:
    """
    Flusso:

    1. Filtra tick trigger:
       - market_state imbalance (up/down)
       - location flag attivo (LVN o valley)
       - big bubble (TradeType + Volume >= soglia)
       - node_cvd coerente con direzione (solo se use_node_cvd=True):
           long  → node_cvd > 0  (buyers dominano storicamente quel nodo VP)
           short → node_cvd < 0  (sellers dominano storicamente quel nodo VP)

    2. Per ogni barra 1-min tieni la PRIMA bubble (ordine Sequence)

    3. Direzione segnale da market_state

    4. Conferma chiusura barra:
       - Long:  current_bar_close > AskPrice AND close > open
       - Short: current_bar_close < BidPrice AND close < open

    5. Entry = next_bar_open
       Entry Index = primo Index della next_bar_datetime (via join)

    6. Stop loss = current_bar_high/low ± STOP_TICKS
    """

    return (
        df

        # ── 1. Tick trigger ──────────────────────────────────────
        .filter(
            (
                # Long trigger
                (pl.col("market_state") == "imbalance_up") &
                (pl.col(location_col)) &
                (pl.col("TradeType") == 1) &
                (pl.col("Volume") >= BIG_ORDER_THRESHOLD) &
                (pl.col("node_cvd") > 0 if use_node_cvd else pl.lit(True))
            ) | (
                # Short trigger
                (pl.col("market_state") == "imbalance_down") &
                (pl.col(location_col)) &
                (pl.col("TradeType") == 2) &
                (pl.col("Volume") >= BIG_ORDER_THRESHOLD) &
                (pl.col("node_cvd") < 0 if use_node_cvd else pl.lit(True))
            )
        )

        # ── 2. Prima bubble della barra (ordine naturale) ────────
        .sort(["current_bar_datetime", "Sequence"])
        .unique(subset=["current_bar_datetime"], keep="first")
        .sort("current_bar_datetime")

        # ── 3. Direzione ─────────────────────────────────────────
        .with_columns(
            pl.when(pl.col("market_state") == "imbalance_up")
              .then(pl.lit("long"))
              .otherwise(pl.lit("short"))
              .alias("signal_direction")
        )

        # ── 4. Conferma chiusura barra ───────────────────────────
        .with_columns(
            pl.when(pl.col("signal_direction") == "long")
              .then(
                  (pl.col("current_bar_close") > pl.col("AskPrice")) &
                  (pl.col("current_bar_close") > pl.col("current_bar_open"))
              )
              .otherwise(
                  (pl.col("current_bar_close") < pl.col("BidPrice")) &
                  (pl.col("current_bar_close") < pl.col("current_bar_open"))
              )
              .alias("bar_confirmed")
        )
        .filter(pl.col("bar_confirmed"))

        # ── 5. Entry price + Entry Index ─────────────────────────
        .with_columns(
            pl.col("next_bar_open").alias("entry_price"),
        )
        .join(
            bar_first_index.rename({"first_index": "entry_index"}),
            left_on="next_bar_datetime",
            right_on="current_bar_datetime",
            how="left",
        )

        # ── 6. Stop loss ─────────────────────────────────────────
        .with_columns(
            pl.when(pl.col("signal_direction") == "long")
              .then(pl.col("current_bar_high") + STOP_TICKS * TICK_SIZE)
              .otherwise(pl.col("current_bar_low") - STOP_TICKS * TICK_SIZE)
              .alias("stop_loss"),
        )

        # ── 7. Colonne output ────────────────────────────────────
        .select([
            pl.col("Index").alias("signal_index"),  # Index tick trigger
            pl.col("entry_index").alias("Index"),   # Index primo tick barra entry
            "current_bar_datetime",
            "next_bar_datetime",
            "Hour",
            "signal_direction",
            "market_state",
            "TradeType",
            "entry_price",
            "stop_loss",
            "Prev_POC",             # target naturale del modello
            "Volume",               # volume bubble trigger
            "CVD",                  # pressione cumulativa sessione ETH
            "current_bar_cvd",      # pressione barra 1-min al momento del trigger
            "node_cvd",             # chi domina storicamente quel nodo VP (usato come filtro)
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
            "POC",
            "LVN",
            "ValleysPeaks",
        ])
    )


# ─────────────────────────────────────────────
# 4. ANALISI COMPARATIVA VARIANTI
# ─────────────────────────────────────────────

def compare_variants(all_signals: pl.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("CONFRONTO VARIANTI — DISTRIBUZIONE SEGNALI")
    print("=" * 60)

    summary = (
        all_signals
        .group_by("variant")
        .agg([
            pl.len().alias("total_signals"),
            (pl.col("signal_direction") == "long").sum().alias("longs"),
            (pl.col("signal_direction") == "short").sum().alias("shorts"),
            pl.col("Volume").mean().round(2).alias("avg_trigger_vol"),
            pl.col("Volume").median().alias("median_trigger_vol"),
            pl.col("Volume").max().alias("max_trigger_vol"),
            pl.col("Index").null_count().alias("entry_index_nulls"),
        ])
        .sort("variant")
    )
    print(summary)

    print("\n--- Distribuzione oraria per variante ---")
    hourly = (
        all_signals
        .group_by(["variant", "Hour"])
        .agg(pl.len().alias("count"))
        .sort(["variant", "Hour"])
    )
    print(hourly)

    print("\n--- Distribuzione giornaliera per variante ---")
    daily = (
        all_signals
        .with_columns(
            # Gestisce sia Datetime che string ISO (es. "2025-12-10T09:00:00")
            pl.col("current_bar_datetime")
            .cast(pl.String)
            .str.slice(0, 10)
            .alias("date")
        )
        .group_by(["variant", "date"])
        .agg(pl.len().alias("count"))
        .sort(["variant", "date"])
    )
    print(daily)

    print("\n--- node_cvd stats per variante e direzione ---")
    cvd_stats = (
        all_signals
        .group_by(["variant", "signal_direction"])
        .agg([
            pl.col("node_cvd").mean().round(1).alias("node_cvd_mean"),
            pl.col("node_cvd").median().alias("node_cvd_median"),
            pl.col("CVD").mean().round(1).alias("session_cvd_mean"),
            pl.col("current_bar_cvd").mean().round(1).alias("bar_cvd_mean"),
        ])
        .sort(["variant", "signal_direction"])
    )
    print(cvd_stats)


# ─────────────────────────────────────────────
# 5. ESECUZIONE
# ─────────────────────────────────────────────

df_with_location = add_location_flags(df_step_a)
bar_first_index = build_bar_first_index(df_with_location)

all_variant_signals = []

for variant_name, (location_col, description) in LOCATION_VARIANTS.items():
    print(f"\n{'─' * 50}")
    print(f"Variante {variant_name}: {description}")

    use_node_cvd = "no_nodecvd" not in variant_name
    signals = (
        generate_signals(df_with_location, bar_first_index, location_col, use_node_cvd)
        .with_columns(pl.lit(variant_name).alias("variant"))
    )

    total = len(signals)
    if total == 0:
        print("  Nessun segnale generato.")
        continue

    longs  = signals.filter(pl.col("signal_direction") == "long").height
    shorts = signals.filter(pl.col("signal_direction") == "short").height
    avg_vol = signals["Volume"].mean()
    null_ei = signals["Index"].null_count()

    print(f"  Segnali totali:        {total:>5}")
    print(f"  Long:                  {longs:>5}")
    print(f"  Short:                 {shorts:>5}")
    print(f"  Volume medio trigger:  {avg_vol:.2f}")
    if null_ei > 0:
        print(f"  ATTENZIONE Index null: {null_ei} "
              f"(segnali a fine dataset senza barra successiva)")

    all_variant_signals.append(signals)

df_all_signals = pl.concat(all_variant_signals)
compare_variants(df_all_signals)

print(f"\nStep B completato.")
print(f"Totale segnali (tutte varianti): {len(df_all_signals):,}")
print("Variabile disponibile per backtest: df_all_signals")
#endregion

#region ---------------------------- BACK TEST CLAUDE STRATEGY ------------------------
@dataclass
class DynamicTPSLExit(BaseExitStrategy):
    """
    Exit strategy that uses per-signal TP and SL values from dedicated columns.
    
    Expects signals DataFrame to have columns: Index, TradeType, TP_Ticks, SL_Ticks
    """
    signals_df: pd.DataFrame
    tick_size: float = 0.25
    _signal_lookup: Dict[int, tuple] = field(default_factory=dict, init=False, repr=False)
    _current_tp: float = field(default=None, init=False, repr=False)
    _current_sl: float = field(default=None, init=False, repr=False)
    
    def __post_init__(self):
        # Build a lookup dict: signal Index -> (TP_Ticks, SL_Ticks)
        for _, row in self.signals_df.iterrows():
            self._signal_lookup[int(row['Index'])] = (
                float(row['TP_Ticks']),
                float(row['SL_Ticks'])
            )
    
    def on_entry(self, tick: Tick, position: PositionState) -> None:
        """Called when position opens — retrieve TP/SL for this signal."""
        if tick.index in self._signal_lookup:
            self._current_tp, self._current_sl = self._signal_lookup[tick.index]
    
    def on_tick(
        self,
        tick: Tick,
        position: PositionState,
        price_history: np.ndarray,
        indicators: Dict[str, Any],
    ) -> ExitSignal:
        price = tick.price
        entry = position.entry_price
        
        if self._current_tp is None or self._current_sl is None:
            return ExitSignal(should_exit=False)
        
        tp_distance = self._current_tp * self.tick_size
        sl_distance = self._current_sl * self.tick_size
        
        if position.side == Side.LONG:
            if price - entry >= tp_distance:
                return ExitSignal(should_exit=True, reason=ExitReason.TAKE_PROFIT)
            if entry - price >= sl_distance:
                return ExitSignal(should_exit=True, reason=ExitReason.STOP_LOSS)
        
        elif position.side == Side.SHORT:
            if entry - price >= tp_distance:
                return ExitSignal(should_exit=True, reason=ExitReason.TAKE_PROFIT)
            if price - entry >= sl_distance:
                return ExitSignal(should_exit=True, reason=ExitReason.STOP_LOSS)
        
        return ExitSignal(should_exit=False)

# first varian selection
df_all_signals_A = df_all_signals.filter(pl.col("variant") == "A_LVN")
df_all_signals_A_pd = (
    df_all_signals_A.to_pandas()
    .assign(
        TP_Ticks=lambda x: abs(x['Prev_POC'] - x['entry_price']) / tick_size,
        SL_Ticks=lambda x: abs(x['stop_loss'] - x['entry_price']) / tick_size,
    )
)
    
exit_strategy_A = DynamicTPSLExit(signals_df=df_all_signals_A_pd, tick_size=tick_size)

result_A = engine.run(
    data=df_ticks_pd,
    signals=df_all_signals_A_pd,
    exit_strategy=exit_strategy_A
)

result_A.summary()
print(f"\nTrades DataFrame shape: {result_A.trades_df.shape}")
print(result_A.trades_df.head(5).to_string(index=False))

# second variant selection
df_all_signals_B = df_all_signals.filter(pl.col("variant") == "B_valley")
df_all_signals_B_pd = (
    df_all_signals_B.to_pandas()
    .assign(
        TP_Ticks=lambda x: abs(x['Prev_POC'] - x['entry_price']) / tick_size,
        SL_Ticks=lambda x: abs(x['stop_loss'] - x['entry_price']) / tick_size,
    )
)
    
exit_strategy_B = DynamicTPSLExit(signals_df=df_all_signals_B_pd, tick_size=tick_size)

result_B = engine.run(
    data=df_ticks_pd,
    signals=df_all_signals_B_pd,
    exit_strategy=exit_strategy_B
)

result_B.summary()
print(f"\nTrades DataFrame shape: {result_B.trades_df.shape}")
print(result_B.trades_df.head(5).to_string(index=False))

# third variant selection
df_all_signals_C = df_all_signals.filter(pl.col("variant") == "B_valley_nodecvd")
df_all_signals_C_pd = (
    df_all_signals_C.to_pandas()
    .assign(
        TP_Ticks=lambda x: abs(x['Prev_POC'] - x['entry_price']) / tick_size,
        SL_Ticks=lambda x: abs(x['stop_loss'] - x['entry_price']) / tick_size,
    )
)
    

exit_strategy_C = DynamicTPSLExit(signals_df=df_all_signals_C_pd, tick_size=tick_size)

result_C = engine.run(
    data=df_ticks_pd,
    signals=df_all_signals_C_pd,
    exit_strategy=exit_strategy_C
)

result_C.summary()
print(f"\nTrades DataFrame shape: {result_C.trades_df.shape}")
print(result_C.trades_df.head(5).to_string(index=False))

#endregion
 