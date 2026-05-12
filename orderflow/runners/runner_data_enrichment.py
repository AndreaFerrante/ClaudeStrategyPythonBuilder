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
#from orderflow.compressor import compress_to_volume_bars

# See 100 elements in polars tables while printing !
pl.Config.set_tbl_rows(100)


#region ------------------------------- CONFIG ----------------------------------------
FOLDER         = r"C:/Users/ZH-APPLICATION/Documents/Pycharm/Orderflow/sources/ES/unarchive/"
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
# files = [FOLDER + '202502_checkpoint_1_1.parquet', FOLDER + '202502_checkpoint_1_2.parquet']
# df_ticks.write_parquet(FOLDER + '202506_checkpoint_1.parquet', compression="snappy")
# ------------------------------------------------------------------------------
# CHECKPOINT 1: load intermediate file -----------------------------------------
# df_ticks_lazy = pl.scan_parquet(files)
# df_ticks_lazy = pl.scan_parquet(FOLDER + '202506_checkpoint_1.parquet')
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
# df_ticks.write_parquet(FOLDER + '202506_checkpoint_2.parquet', compression="snappy")
# ------------------------------------------------------------------------------
# CHECKPOINT 2: load intermediate file -----------------------------------------
# df_ticks_lazy = pl.scan_parquet(FOLDER + '202506_checkpoint_2.parquet')
# df_ticks = df_ticks_lazy.collect()
# ------------------------------------------------------------------------------
cols_needed = ['Price', 'Volume', 'SessionType']
df_ticks_pd = df_ticks.select(cols_needed).to_pandas() # converto to pandas only a subset of the df
df_ticks = df_ticks.with_columns(VA_Areas     = get_volume_profile_areas(df_ticks_pd))
df_ticks = df_ticks.with_columns(ValleysPeaks = get_volume_profile_peaks_valleys(df_ticks_pd))

# CHECKPOINT 3: save intermediate file -----------------------------------------
# df_ticks.write_parquet(FOLDER + '202506_checkpoint_3.parquet', compression="snappy")
# CHECKPOINT 3: load intermediate file -----------------------------------------
# df_ticks_lazy = pl.scan_parquet(FOLDER + '202506_checkpoint_3.parquet')
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
# df_ticks.write_parquet(FOLDER + '202506_checkpoint_4.parquet', compression="snappy")
# ------------------------------------------------------------------------------
# CHECKPOINT 4: load intermediate file -----------------------------------------
# df_ticks_lazy = pl.scan_parquet(FOLDER + 'ES202601_checkpoint_4.parquet')
# df_ticks = df_ticks_lazy.collect()
# ------------------------------------------------------------------------------
#endregion

#region  ------------------------------- ENRICH TICKS 2 --------------------------------
# add 1 minute bars info
df_bars = compress_to_minute_bars_pl(df_ticks,
                                             win_compression = '1m',
                                             time_column="Datetime")

# -------------------------------------------------------------------------------------
# df_bars = compress_to_volume_bars_pl(df_ticks, 500, False)

# df_ticks = df_ticks.with_columns(pl.col("Datetime").set_sorted())

# df_bars = df_bars.with_columns(pl.col("DatetimeOpen").set_sorted())

# df_bars = df_bars.with_columns(pl.col("DatetimeOpen").dt.replace_time_zone(None))
# df_bars = df_bars.with_columns(pl.col("DatetimeClose").dt.replace_time_zone(None))

# df_bars_renamed = df_bars.rename({
#     "DatetimeOpen": "DatetimeOpen_volbar",
#     "DatetimeClose": "DatetimeClose_volbar",
#     "Open": "Open_volbar",
#     "High": "High_volbar",
#     "Low": "Low_volbar",
#     "Close": "Close_volbar",
#     "Volume": "Volume_volbar",
#     "AskVolume": "AskVolume_volbar",
#     "BidVolume": "BidVolume_volbar",
#     "NumberOfTrades": "NumberOfTrades_volbar",
# })

# df_ticks_with_bars = df_ticks.join_asof(
#     df_bars_renamed,
#     left_on="Datetime",
#     right_on="DatetimeOpen_volbar",
#     strategy="backward",
# )
# -------------------------------------------------------------------------------------

df_bars = df_bars.with_columns(
    pl.col("Datetime").dt.replace_time_zone(None)  # ← Remove timezone
)
df_bars = df_bars.with_columns([
        pl.col("Datetime").alias("current_bar_datetime")
    ])
rename_map = {
        col: f"current_bar_{col.lower()}"
        for col in df_bars.columns
        if col not in ["Datetime", "current_bar_datetime"]
    }

df_bars_renamed = df_bars.rename(rename_map)

df_bars_renamed_extended = df_bars_renamed.with_columns([
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
        df_bars_renamed_extended,
        left_on="Datetime",
        right_on="Datetime",
        strategy="backward"
    )


# CHECKPOINT 5: save intermediate file -----------------------------------------
# df_ticks_with_bars.write_parquet(FOLDER + '202506_checkpoint_5.parquet', compression="snappy")
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

# CHECKPOINT 6: save intermediate file -----------------------------------------
# df_ticks_with_bars.write_parquet(FOLDER + 'checkpoint_6.parquet', compression="snappy")
# ------------------------------------------------------------------------------
#endregion-------
