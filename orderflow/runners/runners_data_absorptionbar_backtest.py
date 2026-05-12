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
from orderflow.backtester.exits import CompositeExit, FixedTPSLExit, TrailingStopExit, \
    DynamicTPSLExit, HourBasedExit

# See 100 elements in polars tables while printing !
pl.Config.set_tbl_rows(100)


#region ------------------------------- CONFIG ----------------------------------------
FOLDER         = r"C:/Users/ZH-APPLICATION/Documents/Pycharm/Orderflow/sources/ES/"
TICKER         = "ES"
MARKET         = "CME"
SEPARATOR      = ";"
EXTENSION      = ".txt"

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

def build_volume_bar_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Costruisce un DataFrame di barre volume 500 arricchito.
    Ogni riga = una barra volume con:
      - OHLCV dalla barra volume
      - VA position, ValleysPeaks, SessionType, POC, Prev_POC
        dal LAST tick della barra (logica chiusura)
      - volume_ma20: moving average 20 barre del volume,
        che riparte ad ogni inizio sessione ETH
    """
    return (
        df
        .sort(["DatetimeOpen_volbar", "Sequence"])
        .group_by("DatetimeOpen_volbar")
        .agg([
            # ── Barra volume ──────────────────────────────
            pl.col("DatetimeClose_volbar").last(),
            pl.col("Open_volbar").last(),
            pl.col("High_volbar").last(),
            pl.col("Low_volbar").last(),
            pl.col("Close_volbar").last(),
            pl.col("Volume_volbar").last(),
            pl.col("AskVolume_volbar").last(),
            pl.col("BidVolume_volbar").last(),
            pl.col("NumberOfTrades_volbar").last(),
            pl.col("Index").last(),

            # ── Stato mercato al last tick ────────────────
            pl.col("VA_Areas").last().alias("va_areas_close"),
            pl.col("ValleysPeaks").last().alias("valley_peaks_close"),
            pl.col("SessionType").last().alias("session_type_close"),
            pl.col("POC").last().alias("poc_close"),
            pl.col("Prev_POC").last().alias("prev_poc_close"),
        ])
        .rename({"DatetimeOpen_volbar": "bar_open"})
        .with_columns(
            # VA position semplificata
            pl.when(pl.col("va_areas_close").is_in(["VA", "PO"]))
              .then(pl.lit("inside"))
              .otherwise(pl.lit("outside"))
              .alias("va_position"),
        )
        .sort("bar_open")

        # ── Session ID: incrementa ad ogni inizio ETH ─────
        .with_columns(
            (
                (pl.col("session_type_close") == "ETH") &
                (pl.col("session_type_close").shift(1) != "ETH")
            ).cast(pl.Int32).cum_sum().alias("session_id")
        )

        # ── Volume MA20 per sessione ───────────────────────
        .with_columns(
            pl.col("Volume_volbar")
              .rolling_mean(window_size=20)
              .over("session_id")
              .alias("volume_ma20")
        )
        .drop("session_id")
    )

def add_absorption_signals(
    df: pl.DataFrame,
    n_bars: int = 3,
) -> pl.DataFrame:
    """
    Replica la logica C++ di absorption su barre volume 500.

    Segnali:
    - absorbBuy:  upTrend (prezzi + delta crescenti) + barra down + delta positivo
                  solo se fuori VA sopra il POC (imbalance_up)
    - absorbSell: downTrend (prezzi + delta decrescenti) + barra up + delta negativo
                  solo se fuori VA sotto il POC (imbalance_down)

    Filtro sessione: 08:30 - 15:00 CT
    """
    return (
        df

        # ── Filtro sessione ───────────────────────────────────
        .with_columns(
            pl.col("bar_open").dt.hour().alias("_hour"),
            pl.col("bar_open").dt.minute().alias("_minute"),
        )
        .filter(
            (
                (pl.col("_hour") == 8) & (pl.col("_minute") >= 30)
            ) | (
                pl.col("_hour") > 8
            )
        )
        .filter(pl.col("_hour") < 15)
        .drop(["_hour", "_minute"])

        # ── Delta per barra ───────────────────────────────────
        # CD_Close > CD_Open  ≡  AskVolume > BidVolume (buyers dominano la barra)
        .with_columns(
            (pl.col("AskVolume_volbar") - pl.col("BidVolume_volbar"))
            .alias("bar_delta"),
        )

        # ── Flag per barra singola ────────────────────────────
        .with_columns(
            # Prezzo su/giù rispetto barra precedente
            (pl.col("Close_volbar") > pl.col("Close_volbar").shift(1))
            .cast(pl.Int32).alias("_price_up_bar"),

            (pl.col("Close_volbar") < pl.col("Close_volbar").shift(1))
            .cast(pl.Int32).alias("_price_down_bar"),

            # Delta su/giù nella barra
            (pl.col("bar_delta") > 0)
            .cast(pl.Int32).alias("_delta_up_bar"),

            (pl.col("bar_delta") < 0)
            .cast(pl.Int32).alias("_delta_down_bar"),
        )

        # ── Trend sulle ultime N barre ────────────────────────
        # Se rolling_sum == n_bars → tutte le barre soddisfacevano la condizione
        .with_columns(
            (
                pl.col("_price_up_bar")
                  .shift(1)                        # esclude barra corrente
                  .rolling_sum(window_size=n_bars)
                  == n_bars
            ).alias("price_up"),

            (
                pl.col("_price_down_bar")
                  .shift(1)
                  .rolling_sum(window_size=n_bars)
                  == n_bars
            ).alias("price_down"),

            (
                pl.col("_delta_up_bar")
                  .shift(1)
                  .rolling_sum(window_size=n_bars)
                  == n_bars
            ).alias("delta_up"),

            (
                pl.col("_delta_down_bar")
                  .shift(1)
                  .rolling_sum(window_size=n_bars)
                  == n_bars
            ).alias("delta_down"),
        )

        # ── Trend composito ───────────────────────────────────
        .with_columns(
            (pl.col("price_up")   & pl.col("delta_up"))  .alias("up_trend"),
            (pl.col("price_down") & pl.col("delta_down")).alias("down_trend"),
        )

        # ── Volume spike (barra precedente * 1.5 > MA corrente) ──
        .with_columns(
            (pl.col("Volume_volbar").shift(1) * 1.5 > pl.col("volume_ma20"))
            .alias("volume_spike"),
        )

        # ── Barra corrente ────────────────────────────────────
        .with_columns(
            (pl.col("Close_volbar") > pl.col("Open_volbar")).alias("is_up_bar"),
            (pl.col("Close_volbar") < pl.col("Open_volbar")).alias("is_down_bar"),
        )

        # ── Imbalance direzionale (logica Valentini) ──────────
        # outside VA + sopra POC → imbalance_up  → cerchiamo absorbBuy
        # outside VA + sotto POC → imbalance_down → cerchiamo absorbSell
        .with_columns(
            (
                (pl.col("va_position") == "outside") &
                (pl.col("Close_volbar") > pl.col("poc_close"))
            ).alias("imbalance_up"),

            (
                (pl.col("va_position") == "outside") &
                (pl.col("Close_volbar") < pl.col("poc_close"))
            ).alias("imbalance_down"),
        )

        # ── Segnali finali ────────────────────────────────────
        .with_columns(
            (
                pl.col("up_trend") &
                pl.col("is_down_bar") &
                (pl.col("bar_delta") > 0) &
                pl.col("imbalance_up") &
                pl.col("volume_spike")
            ).alias("signal_short"),  # absorbBuy → SHORT

            (
                pl.col("down_trend") &
                pl.col("is_up_bar") &
                (pl.col("bar_delta") < 0) &
                pl.col("imbalance_down") &
                pl.col("volume_spike")
            ).alias("signal_long"),   # absorbSell → LONG
        )

        # ── Pulizia colonne temporanee ────────────────────────
        .drop([
            "_price_up_bar", "_price_down_bar",
            "_delta_up_bar", "_delta_down_bar",
        ])
    )

df_ticks_with_bars = pl.read_parquet(
    FOLDER + 'ES202601_checkpoint_5_bar_500.parquet'
)

df_bars = build_volume_bar_df(df_ticks_with_bars)

N_TREND_BARS = 3
df_signals = (
        add_absorption_signals(df_bars, N_TREND_BARS)
    ).filter(
        (pl.col('signal_short') == True) |
        (pl.col('signal_long') >= SESSION_START_MINUTE)
    )
