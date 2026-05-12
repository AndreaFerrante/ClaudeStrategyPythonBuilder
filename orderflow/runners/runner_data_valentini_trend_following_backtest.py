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
FOLDER         = r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/ES/parquet/"
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

engine = BacktestEngine(
        tick_size=tick_size,
        tick_value=tick_value,
        commission=0.9,
        n_contracts=1,
        slippage_model=SlippageModel(mode=SlippageMode.UNIFORM, max_ticks=2, seed=42),
        progress_bar=False,
    )
#endregion

#region ----------------- FIND STRATEGY WITH CLAUDE (NO BLOCKS) -----------------------
SESSION_START_HOUR = 8
SESSION_START_MINUTE = 30
SESSION_END_HOUR = 15
SESSION_END_MINUTE = 0

# Filtro contratti per "Big Bubble" (aggression trigger)
# Fabio dice 30 contratti su NQ 1-min → ES è meno volatile,
# 20 è un punto di partenza ragionevole da validare sui dati
BIG_ORDER_THRESHOLD = 50

COLUMNS_NEEDED = [
    # ── Identificazione e ordinamento ────────────────────────
    "Index",                    # riferimento backtest engine
    "Sequence",                 # ordinamento naturale tick dentro la barra
    "Date",
    "Time",
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
    "Session_High",
    "Session_Low",
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
    FOLDER + '202506_ES.parquet',
    columns=COLUMNS_NEEDED,
)

print(f"Shape: {df_ticks_with_bars.shape}")
print(f"Memoria stimata: {df_ticks_with_bars.estimated_size('mb'):.1f} MB")

# ─────────────────────────────────────────────
# 1. FILTRO SESSIONE RTH OPERATIVA
# ─────────────────────────────────────────────
 
def filter_operative_session(df: pl.DataFrame) -> pl.DataFrame:
    """
    Filtra i tick nella finestra operativa NY:
    - SessionType == RTH
    - Datetime tra 08:30 e 15:00 CT
    - Esclude prima sessione dove Prev_POC == 0
    """
    return (
        df
        .filter(pl.col("SessionType") == "RTH")
        .filter(pl.col("Prev_POC") > 0)
        .with_columns(
            pl.col("Datetime").dt.minute().alias("_minute")
        )
        .filter(
            (
                (pl.col("Hour") == SESSION_START_HOUR) &
                (pl.col("_minute") >= SESSION_START_MINUTE)
            ) |
            (pl.col("Hour") > SESSION_START_HOUR)
        )
        .filter(pl.col("Hour") < SESSION_END_HOUR)
        .drop("_minute")
    )


# ─────────────────────────────────────────────
# 2. CVD DIREZIONALE
# ─────────────────────────────────────────────
 
def add_cvd_directional(df: pl.DataFrame) -> pl.DataFrame:
    """
    CVD = CD_Bid - CD_Ask (cumulativo sessione ETH)
 
    Convenzione corretta:
      TradeType=2 (Ask Trade = BUY aggression)  → tick_delta positivo
      TradeType=1 (Bid Trade = SELL aggression) → tick_delta negativo
    """
    return df.with_columns(
        # CVD cumulativo sessione ETH
        (pl.col("CD_Bid") - pl.col("CD_Ask"))
        .alias("CVD"),
 
        # CVD barra 1-min corrente
        (pl.col("current_bar_bidvolume") - pl.col("current_bar_askvolume"))
        .alias("current_bar_cvd"),
 
        # CVD nodo Volume Profile
        (pl.col("Node_Bid_Volume") - pl.col("Node_Ask_Volume"))
        .alias("node_cvd"),
 
        # Delta istantaneo tick
        # TradeType=2 = buy su ask → positivo
        # TradeType=1 = sell su bid → negativo
        pl.when(pl.col("TradeType") == 2)
          .then(pl.col("Volume"))
          .when(pl.col("TradeType") == 1)
          .then(-pl.col("Volume"))
          .otherwise(0)
          .alias("tick_delta"),
    )

# ─────────────────────────────────────────────
# 3. MARKET STATE CLASSIFICATION
# ─────────────────────────────────────────────
 
def add_market_state(df: pl.DataFrame) -> pl.DataFrame:
    """
    Market State per ogni tick.
 
    - BALANCE:             VA_Areas in ('VA', 'PO') → non si tradea
    - IMBALANCE_UP:        fuori VA, Price > POC, Prev_POC > Price → long
    - IMBALANCE_DOWN:      fuori VA, Price < POC, Prev_POC < Price → short
    - IMBALANCE_NO_TARGET: imbalance ma POC prev non allineato → skip
    """
    return df.with_columns(
        pl.when(
            pl.col("VA_Areas").is_in(["VA", "PO"])
        )
        .then(pl.lit("balance"))
 
        .when(
            (pl.col("VA_Areas") == "na") &
            (pl.col("Price") > pl.col("POC")) &
            (pl.col("Prev_POC") > pl.col("Price"))
        )
        .then(pl.lit("imbalance_up"))
 
        .when(
            (pl.col("VA_Areas") == "na") &
            (pl.col("Price") < pl.col("POC")) &
            (pl.col("Prev_POC") < pl.col("Price"))
        )
        .then(pl.lit("imbalance_down"))
 
        .otherwise(pl.lit("imbalance_no_target"))
        .alias("market_state")
    )


# ─────────────────────────────────────────────
# 4. FLAG AGGRESSIONE (Big Bubble)
# ─────────────────────────────────────────────
 
def add_aggression_flag(df: pl.DataFrame, threshold: int = BIG_ORDER_THRESHOLD) -> pl.DataFrame:
    """
    Convenzione corretta:
      big_ask (BUY aggression)  = TradeType==2 (Ask Trade)
      big_bid (SELL aggression) = TradeType==1 (Bid Trade)
    """
    return df.with_columns(
        (
            (pl.col("TradeType") == 2) &
            (pl.col("Volume") >= threshold)
        ).alias("big_ask"),  # buy aggression
 
        (
            (pl.col("TradeType") == 1) &
            (pl.col("Volume") >= threshold)
        ).alias("big_bid"),  # sell aggression
    )

# ─────────────────────────────────────────────
# 5. Prev Session Range
# ─────────────────────────────────────────────
def add_prev_session_range(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcola il range della sessione ETH precedente per ogni tick.
    Stesso approccio di Prev_POC — prende i valori all'ultimo tick
    di ogni sessione e li propaga alla sessione successiva.
    
    prev_session_range = Session_High - Session_Low della sessione precedente
    """
    return (
        df
        .with_columns(
            # Flag ultimo tick di ogni sessione ETH
            (
                (pl.col("SessionType") == "ETH") &
                (pl.col("SessionType").shift(-1) != "ETH")
            ).alias("_is_last_eth_tick")
        )
        .with_columns(
            # Range sessione corrente
            (pl.col("Session_High") - pl.col("Session_Low"))
            .alias("_session_range")
        )
        .with_columns(
            # Propaga il range all'ultima riga ETH, null altrove
            pl.when(pl.col("_is_last_eth_tick"))
              .then(pl.col("_session_range"))
              .otherwise(None)
              .alias("_prev_range_raw")
        )
        .with_columns(
            # Forward fill → ogni tick conosce il range della sessione precedente
            pl.col("_prev_range_raw")
              .shift(1)
              .forward_fill()
              .alias("prev_session_range")
        )
        .drop(["_is_last_eth_tick", "_session_range", "_prev_range_raw"])
    )


# ─────────────────────────────────────────────
# 5. PIPELINE COMPLETA STEP A
# ─────────────────────────────────────────────
 
def run_step_a(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .pipe(add_prev_session_range) # this step before hours filtering
        .pipe(filter_operative_session)
        .pipe(add_cvd_directional)
        .pipe(add_market_state)
        .pipe(add_aggression_flag)
    )


# ─────────────────────────────────────────────
# 6. ESPLORAZIONE DISTRIBUZIONI
# ─────────────────────────────────────────────

def explore(df: pl.DataFrame) -> None:
    print("=" * 60)
    print("STEP A — ESPLORAZIONE DATI")
    print("=" * 60)
 
    print(f"\nTick totali dopo filtro sessione: {len(df):,}")
 
    print("\n--- Market State Distribution ---")
    print(
        df.group_by("market_state")
          .agg(pl.len().alias("count"))
          .with_columns(
              (pl.col("count") / pl.col("count").sum() * 100)
              .round(2).alias("pct")
          )
          .sort("count", descending=True)
    )
 
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
 
    print("\n--- Big Orders count per soglia Volume ---")
    for threshold in [10, 20, 30, 50, 100]:
        n = df.filter(pl.col("Volume") >= threshold).height
        pct = n / len(df) * 100
        print(f"  >= {threshold:>3} contratti: {n:>6,} tick ({pct:.3f}%)")
 
    print("\n--- LVN Flag Distribution ---")
    print(
        df.group_by("LVN")
          .agg(pl.len().alias("count"))
          .with_columns(
              (pl.col("count") / pl.col("count").sum() * 100)
              .round(2).alias("pct")
          )
    )
 
    print("\n--- Setup Potenziali (imbalance + ValleysPeaks) ---")
    potential_long = df.filter(
        (pl.col("market_state") == "imbalance_up") &
        (pl.col("ValleysPeaks").is_in([-2, -1]))
    ).height
    potential_short = df.filter(
        (pl.col("market_state") == "imbalance_down") &
        (pl.col("ValleysPeaks").is_in([-2, -1]))
    ).height
    print(f"  Long setups  (imbalance_up + valley):   {potential_long:,} tick")
    print(f"  Short setups (imbalance_down + valley):  {potential_short:,} tick")
 
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
    print("Step A completato. Pronto per Step B.")
    print("=" * 60)


# ─────────────────────────────────────────────
# 7. ESECUZIONE
# ─────────────────────────────────────────────
 
df_step_a = run_step_a(df_ticks_with_bars)
explore(df_step_a)


"""
Step B — Signal Generation
Modello Trend Following NY Session (Fabio Valentino)
Ticker: ES Futures
 
Convenzione TradeType (Sierra Chart):
  TradeType=1 = Bid Trade = SELL aggression → trigger SHORT
  TradeType=2 = Ask Trade = BUY  aggression → trigger LONG
 
Varianti:
  A_LVN:    location = LVN solo
  B_valley: location = ValleysPeaks (-2,-1) solo
  C_LVN_valley: location LVN and ValleysPeaks (-2,-1)
 
Filtro bubble opposte:
  Dopo il tick trigger, se nella stessa barra appare una big bubble
  nella direzione opposta (Volume >= threshold) → segnale invalidato.
"""
 
 
# ─────────────────────────────────────────────
# COSTANTI
# ─────────────────────────────────────────────
 
STOP_TICKS          = 2     # tick oltre il prezzo della bubble trigger
TICK_SIZE           = tick_size

LOCATION_VARIANTS = {
    "A_LVN":    ("loc_A", "LVN solo"),
    "B_valley": ("loc_B", "ValleysPeaks (-2,-1) solo"),
    "C_LVN_valley": ("loc_C", "LVN and ValleysPeaks (-2,-1)"),
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

        ((pl.col("LVN") == 1) & (pl.col("ValleysPeaks").is_in([-2, -1])))
        .alias("loc_C"),
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
# 3. FILTRO BUBBLE OPPOSTE
# ─────────────────────────────────────────────
 
def filter_no_opposing_bubble(
    triggers: pl.DataFrame,
    df_full: pl.DataFrame,
) -> pl.DataFrame:
    """
    Invalida barre dove dopo il trigger appare una big bubble opposta.
 
    Convenzione corretta:
      LONG trigger  (TradeType=2): opposta = TradeType=1 (sell aggression)
      SHORT trigger (TradeType=1): opposta = TradeType=2 (buy aggression)
      opposing_type = 3 - trigger_type  (invariante: 1↔2)
    """
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
            (pl.col("opp_seq") > pl.col("trigger_seq")) &
            (pl.col("opp_type") == pl.col("opposing_type"))
        )
        .select("current_bar_datetime")
        .unique()
    )
 
    return triggers.join(invalidated_bars, on="current_bar_datetime", how="anti")


# ─────────────────────────────────────────────
# 4. PIPELINE SEGNALI PER UNA VARIANTE
# ─────────────────────────────────────────────
 
def generate_signals(
    df: pl.DataFrame,
    bar_first_index: pl.DataFrame,
    location_col: str,
) -> pl.DataFrame:
    """
    Flusso:
 
    1. Tick trigger:
       LONG:  imbalance_up  + location + TradeType==2 (BUY)  + Volume>=10
       SHORT: imbalance_down + location + TradeType==1 (SELL) + Volume>=10
 
    2. Prima bubble per barra (ordine Sequence)
 
    3. Filtro bubble opposte
 
    4. Direzione da market_state
 
    5. Conferma chiusura barra:
       LONG:  close > AskPrice AND close > open
       SHORT: close < BidPrice AND close < open
 
    6. Entry = next_bar_open
       Entry Index = primo tick della next_bar_datetime
 
    7. Filtro allineamento Prev_POC:
       LONG:  entry_price < Prev_POC
       SHORT: entry_price > Prev_POC
 
    8. Stop loss:
       LONG:  AskPrice - STOP_TICKS * TICK_SIZE
       SHORT: BidPrice + STOP_TICKS * TICK_SIZE
    """
 
    # ── 1. Tick trigger ──────────────────────────────────────
    triggers = (
        df
        .filter(
            (
                # LONG: imbalance_up + BUY aggression (TradeType=2)
                (pl.col("market_state") == "imbalance_up") &
                (pl.col(location_col)) &
                (pl.col("TradeType") == 2) &
                (pl.col("Volume") >= BIG_ORDER_THRESHOLD)
            ) | (
                # SHORT: imbalance_down + SELL aggression (TradeType=1)
                (pl.col("market_state") == "imbalance_down") &
                (pl.col(location_col)) &
                (pl.col("TradeType") == 1) &
                (pl.col("Volume") >= BIG_ORDER_THRESHOLD)
            )
        )
 
        # ── 2. Prima bubble per barra ────────────────────────
        .sort(["current_bar_datetime", "Sequence"])
        .unique(subset=["current_bar_datetime"], keep="first")
        .sort("current_bar_datetime")
    )
 
    # ── 3. Filtro bubble opposte ─────────────────────────────
    triggers = filter_no_opposing_bubble(triggers, df)
 
    return (
        triggers
 
        # ── 4. Direzione ─────────────────────────────────────
        .with_columns(
            pl.when(pl.col("market_state") == "imbalance_up")
              .then(pl.lit("long"))
              .otherwise(pl.lit("short"))
              .alias("signal_direction")
        )
 
        # ── 5. Conferma chiusura barra ───────────────────────
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
 
        # ── 6. Entry price + Entry Index ─────────────────────
        .with_columns(
            pl.col("next_bar_open").alias("entry_price"),
        )
        .join(
            bar_first_index.rename({"first_index": "entry_index"}),
            left_on="next_bar_datetime",
            right_on="current_bar_datetime",
            how="left",
        )
 
        # ── 7. Filtro allineamento Prev_POC ──────────────────
        .filter(
            pl.when(pl.col("signal_direction") == "long")
              .then(pl.col("entry_price") < pl.col("Prev_POC"))
              .otherwise(pl.col("entry_price") > pl.col("Prev_POC"))
        )
 
        # ── 8. Stop loss ──────────────────────────────────────
        # LONG:  AskPrice = prezzo su cui è avvenuta la BUY aggression
        # SHORT: BidPrice = prezzo su cui è avvenuta la SELL aggression
        .with_columns(
            pl.when(pl.col("signal_direction") == "long")
              .then(pl.col("AskPrice") - STOP_TICKS * TICK_SIZE)
              .otherwise(pl.col("BidPrice") + STOP_TICKS * TICK_SIZE)
              .alias("stop_loss"),
        )
 
        # ── 9. Colonne output ─────────────────────────────────
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
            "POC",
            "LVN",
            "ValleysPeaks",
            "prev_session_range"
        ])
    )


# ─────────────────────────────────────────────
# 5. ANALISI COMPARATIVA VARIANTI
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


# ─────────────────────────────────────────────
# 6. ESECUZIONE
# ─────────────────────────────────────────────

df_with_location = add_location_flags(df_step_a)
bar_first_index = build_bar_first_index(df_with_location)

all_variant_signals = []

for variant_name, (location_col, description) in LOCATION_VARIANTS.items():
    print(f"\n{'─' * 50}")
    print(f"Variante {variant_name}: {description}")

    signals = (
        generate_signals(df_with_location, bar_first_index, location_col)
        .with_columns(pl.lit(variant_name).alias("variant"))
    ).filter(
    (pl.col("next_bar_datetime").dt.hour() < SESSION_END_HOUR) |
    (
        (pl.col("next_bar_datetime").dt.hour() == SESSION_END_HOUR) &
        (pl.col("next_bar_datetime").dt.minute() < SESSION_END_MINUTE)
    )
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


COLUMNS_NEEDED_DATA = [ 'Date', 'Datetime', 'Index', 'Price', 'SessionType', 'Time' ]
df_ticks_pd = df_ticks_with_bars.select(COLUMNS_NEEDED_DATA).to_pandas()


# first varian selection
df_all_signals_A = df_all_signals.filter(pl.col("variant") == "A_LVN")
df_all_signals_A_pd = (
    df_all_signals_A.to_pandas()
    .assign(
        TP_Ticks=lambda x: abs(x['Prev_POC'] - x['entry_price']) / tick_size,
        SL_Ticks=lambda x: abs(x['stop_loss'] - x['entry_price']) / tick_size,
    )
    .sort_values("Index")  # ← CRITICO
    .reset_index(drop=True)
)

exit_strategy_A = CompositeExit([
    DynamicTPSLExit(signals_df=df_all_signals_A_pd, tick_size=tick_size),
    HourBasedExit(close_hour=15, close_minute=0)  # Forces close at 3 PM
])


result_A = engine.run(
    data=df_ticks_pd,
    signals=df_all_signals_A_pd,
    exit_strategy=exit_strategy_A
)

result_A.summary()
print(f"\nTrades DataFrame shape: {result_A.trades_df.shape}")
print(result_A.trades_df.head(5).to_string(index=False))

result_A = (
    pl.from_pandas(result_A.trades_df)
    .join(
        df_all_signals_A.select(["Index", "prev_session_range", "Volume"]),
        left_on="entry_timestamp",
        right_on="Index",
        how="left"
    )
)

result_A.write_parquet(FOLDER + "trades_A_LVN_202602_v2.parquet")

# second variant selection

# some index can be null at the end of the session. The index here is the index of the next open bar
df_all_signals_B = (
    df_all_signals
    .filter(pl.col("variant") == "B_valley")
    .drop_nulls(subset=["Index"])
)

df_all_signals_B_pd = (
    df_all_signals_B.to_pandas()
    .assign(
        TP_Ticks=lambda x: abs(x['Prev_POC'] - x['entry_price']) / tick_size,
        SL_Ticks=lambda x: abs(x['stop_loss'] - x['entry_price']) / tick_size,
    )
    .sort_values("Index")  # ← CRITICO
    .reset_index(drop=True)
)

exit_strategy_B = CompositeExit([
    DynamicTPSLExit(signals_df=df_all_signals_B_pd, tick_size=tick_size),
    HourBasedExit(close_hour=15, close_minute=0)  # Forces close at 3 PM
])

result_B = engine.run(
    data=df_ticks_pd,
    signals=df_all_signals_B_pd,
    exit_strategy=exit_strategy_B
)

result_B.summary()
print(f"\nTrades DataFrame shape: {result_B.trades_df.shape}")
print(result_B.trades_df.head(5).to_string(index=False))

result_B = (
    pl.from_pandas(result_B.trades_df)
    .join(
        df_all_signals_B.select(["Index", "prev_session_range", "Volume", "Prev_POC"]),
        left_on="entry_timestamp",
        right_on="Index",
        how="left"
    )
)

result_B.write_parquet(FOLDER + "trades_B_valley_202602_v2.parquet")

# third variant selection
df_all_signals_C = (
    df_all_signals
    .filter(pl.col("variant").is_in(["C_LVN_valley"]))
    .drop_nulls(subset=["Index"])
)

df_all_signals_C_pd = (
    df_all_signals_C.to_pandas()
    .assign(
        TP_Ticks=lambda x: abs(x['Prev_POC'] - x['entry_price']) / tick_size,
        SL_Ticks=lambda x: abs(x['stop_loss'] - x['entry_price']) / tick_size,
    )
    .sort_values("Index")  # ← CRITICO
    .reset_index(drop=True)
)

exit_strategy_C = CompositeExit([
    DynamicTPSLExit(signals_df=df_all_signals_C_pd, tick_size=tick_size),
    HourBasedExit(close_hour=15, close_minute=0)  # Forces close at 3 PM
])

result_C = engine.run(
    data=df_ticks_pd,
    signals=df_all_signals_C_pd,
    exit_strategy=exit_strategy_C
)

result_C.summary()
print(f"\nTrades DataFrame shape: {result_C.trades_df.shape}")
print(result_C.trades_df.head(8).to_string(index=False))

result_C.trades_df.to_parquet(FOLDER + "trades_C_LVN_and_Valley_202602_v2.parquet")

#endregion
