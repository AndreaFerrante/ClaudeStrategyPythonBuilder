from dataclasses import dataclass, field
from typing import Any, Dict
import math
from datetime import time

import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

import orderflow.configuration as cf

# from orderflow._volume_factory import get_tickers_in_folder_mem_optim, get_market_evening_session
# from orderflow.volume_profile import get_volume_profile_peaks_valleys, get_daily_high_and_low_by_session, get_dynamic_cumulative_delta_per_session
# from orderflow.volume_profile_kde import gaussian_kde_numba_parallel, get_kde_high_low_price_peaks
# from orderflow.volume_profile import get_volume_profile_areas, get_volume_profile_node_volume, get_daily_session_moving_POC
# from orderflow.auctions import (
#     aggregate_auctions,
#     get_valid_blocks,
#     compute_forward_outcomes,
# )

# from orderflow.compressor import compress_to_minute_bars_pl

# from orderflow.backtester.execution import SlippageMode, SlippageModel
# from orderflow.backtester.engine import BacktestEngine
# from orderflow.backtester.models import ExitSignal, ExitReason, Side, Tick, PositionState
# from orderflow.backtester.exits import BaseExitStrategy, CompositeExit, FixedTPSLExit, TrailingStopExit
import glob

# See 100 elements in polars tables while printing !
pl.Config.set_tbl_rows(100)


#region ------------------------------- CONFIG ----------------------------------------
FOLDER         = r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/sources/ES/Trades/Valentini/mirror_reverting/"
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

# vix integration 
vix = pl.read_parquet(r"C:\Users\Tommy\Documents\PyCharmProjects\Orderflow\sources\VixCentral\/**/*.parquet")
# previous day
vix_lagged = vix.with_columns([
    pl.col("is_contango_f2_minus_f1").shift(1).alias("is_contango_prev"),
    pl.col("regime").shift(1).alias("regime_prev"),
    pl.col("contango_f2_minus_f1").shift(1).alias("contango_f2_minus_f1_prev"),
]).select(["date", "is_contango_prev", "regime_prev", "contango_f2_minus_f1_prev"])

# gex integration
dix = (
    pl.read_csv(r"C:\Users\Tommy\Documents\PyCharmProjects\Orderflow\sources\GEX\dix.csv", try_parse_dates=True)
    .sort("date")
    .with_columns([
        # GEX normalizzato su rolling 252 giorni (1 anno)
        pl.col("gex").rolling_mean(252).alias("gex_ma252"),
        
        # Regime: GEX sopra la sua media = long gamma relativo
        (pl.col("gex") > pl.col("gex").rolling_mean(252))
          .shift(1)
          .alias("is_long_gamma_prev"),
        
        pl.col("dix").shift(1).alias("dix_prev"),
        pl.col("gex").shift(1).alias("gex_prev"),
    ])
)

# -------------------------------- FIRST VARIANT --------------------------------
all_trades_a_lvn = pd.concat([
    pd.read_parquet(f)
    for f in sorted(glob.glob(FOLDER + "trades_MR_B_valley_POC_*.parquet"))
], ignore_index=True)

# re-calculate trade_id
all_trades_a_lvn["trade_id"] = range(1, len(all_trades_a_lvn) + 1)

all_trades_a_lvn = (
    pl.from_pandas(all_trades_a_lvn)
    .with_columns(
        pl.col("entry_datetime").dt.date().alias("date")
    )
    .join(
        vix_lagged.select(["date", "is_contango_prev"]),
        on="date",
        how="left",
    )
    .join(
        dix.select(["date", "is_long_gamma_prev", "gex_prev"]),
        on="date",
        how="left",
    )
    .drop("date")
    .to_pandas()  # riconverti se il resto del codice usa Pandas
)
all_trades_a_lvn["entry_hour"] = pd.to_datetime(all_trades_a_lvn["entry_datetime"]).dt.hour


print(f"Variant A_LVN")
print(f"Trades total: {len(all_trades_a_lvn)}")
print(f"Period: {all_trades_a_lvn['entry_datetime'].min()} → {all_trades_a_lvn['entry_datetime'].max()}")

# Some measures aggregations
wins = (all_trades_a_lvn["net_pnl"] > 0).sum()
losses = (all_trades_a_lvn["net_pnl"] < 0).sum()
total = len(all_trades_a_lvn)
break_even = (all_trades_a_lvn["net_pnl"] == 0).sum()

print(f"\nWin Rate:        {wins/total*100:.1f}%")
print(f"Net P&L totale:  ${all_trades_a_lvn['net_pnl'].sum():.2f}")
print(f"Avg trade P&L:   ${all_trades_a_lvn['net_pnl'].mean():.2f}")
print(f"Profit Factor:   {all_trades_a_lvn[all_trades_a_lvn['net_pnl']>0]['net_pnl'].sum() / abs(all_trades_a_lvn[all_trades_a_lvn['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max Drawdown:    ${all_trades_a_lvn['net_pnl'].cumsum().cummax().sub(all_trades_a_lvn['net_pnl'].cumsum()).max():.2f}")

print(f"Trade totals:    {total}")
print(f"Winners:     {wins}  ({wins/total*100:.1f}%)")
print(f"Losers:      {losses}  ({losses/total*100:.1f}%)")
print(f"Break-even:      {break_even}")

df_losers = all_trades_a_lvn[all_trades_a_lvn["net_pnl"] < 0]
# Quanto tempo vivono i loser?
print(df_losers["ticks_in_trade"].describe())
# MAE dei loser — quanto vanno contro prima di stoppare?
print(df_losers["mae_ticks"].describe())
# MFE dei loser — hanno mai visto profitto prima di stoppare?
print(df_losers["mfe_ticks"].describe())

df_winners = all_trades_a_lvn[all_trades_a_lvn["net_pnl"] > 0]
print("--- WINNER MFE ---")
print(df_winners["mfe_ticks"].describe())
print("\n--- WINNER ora di entrata ---")
df_winners["entry_hour"] = pd.to_datetime(df_winners["entry_datetime"]).dt.hour
print(df_winners.groupby("entry_hour")["net_pnl"].agg(["count", "sum", "mean"]))
print("\n--- WINNER vs LOSER per ora ---")
all_trades_a_lvn["entry_hour"] = pd.to_datetime(all_trades_a_lvn["entry_datetime"]).dt.hour
print(all_trades_a_lvn.groupby("entry_hour").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

df_losers = all_trades_a_lvn[all_trades_a_lvn["net_pnl"] < 0]
print(all_trades_a_lvn[all_trades_a_lvn["mfe_ticks"] > 20][
    ["entry_datetime", "side", "mfe_ticks", "mae_ticks", "ticks_in_trade", "net_pnl"]
])


# ── ANALISI VIX CONTANGO ─────────────────────────────────────
print("\n--- P&L per regime VIX (contango/backwardation) ---")
print(all_trades_a_lvn.groupby("is_contango_prev").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

# ── ANALISI GEX REGIME ───────────────────────────────────────
print("\n--- P&L per regime GEX (long gamma / short gamma) ---")
print(all_trades_a_lvn.groupby("is_long_gamma_prev").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

# ── ANALISI GEX VALORE ───────────────────────────────────────
print("\n--- P&L per quartile GEX ---")
all_trades_a_lvn["gex_quartile"] = pd.qcut(
    all_trades_a_lvn["gex_prev"],
    q=4,
    labels=["Q1_short_gamma", "Q2", "Q3", "Q4_long_gamma"]
)
print(all_trades_a_lvn.groupby("gex_quartile").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

# ── ANALISI COMBINATA VIX + GEX ──────────────────────────────
print("\n--- P&L combinato VIX contango + GEX regime ---")
print(all_trades_a_lvn.groupby(
    ["is_contango_prev", "is_long_gamma_prev"]
).agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

df_a = all_trades_a_lvn.copy()

for ora in [None, [9], [9, 10], [9, 10, 11], [9, 10, 11, 12], [9, 10, 11, 12, 13], 
            [9, 10, 11, 12, 13, 14], [9, 10, 11, 12, 13, 14, 15],
            [10, 11, 12, 13, 14, 15], [12, 13, 14, 15], [13, 14, 15],
            [14, 15], [10, 11, 12, 13, 14], [10, 11, 12, 13,],
            [14], [9, 14]]:
    for gex in [None, False]:
        for vix in [None, False]:
            df_tmp = df_a.copy()
            label = []
            if ora:
                df_tmp = df_tmp[df_tmp["entry_hour"].isin(ora)]
                label.append(f"ore={ora}")
            if gex is not None:
                df_tmp = df_tmp[df_tmp["is_long_gamma_prev"] == gex]
                label.append(f"gex_short")
            if vix is not None:
                df_tmp = df_tmp[df_tmp["is_contango_prev"] == vix]
                label.append(f"backwardation")
            if len(df_tmp) < 8:
                continue
            wins = (df_tmp["net_pnl"] > 0).sum()
            total = len(df_tmp)
            pf = (df_tmp[df_tmp["net_pnl"]>0]["net_pnl"].sum() /
                  abs(df_tmp[df_tmp["net_pnl"]<0]["net_pnl"].sum())) if (df_tmp["net_pnl"]<0).any() else 999
            print(f"{' + '.join(label) or 'nessun filtro':55s} | trade={total:3d} | wr={wins/total*100:4.1f}% | pnl=${df_tmp['net_pnl'].sum():7.0f} | pf={pf:.2f}")
# -------------------------------- SECOND VARIANT --------------------------------
all_trades_B_valley = pd.concat([
    pd.read_parquet(f)
    for f in sorted(glob.glob(FOLDER + "trades_B_valley_*.parquet"))
], ignore_index=True)

# re-calculate trade_id
all_trades_B_valley["trade_id"] = range(1, len(all_trades_B_valley) + 1)

all_trades_B_valley = (
    pl.from_pandas(all_trades_B_valley)
    .with_columns(
        pl.col("entry_datetime").dt.date().alias("date")
    )
    .join(
        vix_lagged.select(["date", "is_contango_prev"]),
        on="date",
        how="left",
    )
    .join(
        dix.select(["date", "is_long_gamma_prev", "gex_prev"]),
        on="date",
        how="left",
    )
    .drop("date")
    .to_pandas()  # riconverti se il resto del codice usa Pandas
)
all_trades_B_valley["entry_hour"] = pd.to_datetime(all_trades_B_valley["entry_datetime"]).dt.hour

# all_trades_B_valley = all_trades_B_valley[
#     (all_trades_B_valley["is_contango_prev"] == False) &
#     #(all_trades_B_valley["is_long_gamma_prev"] == False) &
#     (all_trades_B_valley["entry_hour"].isin([10, 11, 12, 14]))
# ]

print(f"Variant B_valley")
print(f"Trades total: {len(all_trades_B_valley)}")
print(f"Period: {all_trades_B_valley['entry_datetime'].min()} → {all_trades_B_valley['entry_datetime'].max()}")

# Some measures aggregations
wins = (all_trades_B_valley["net_pnl"] > 0).sum()
losses = (all_trades_B_valley["net_pnl"] < 0).sum()
total = len(all_trades_B_valley)
break_even = (all_trades_B_valley["net_pnl"] == 0).sum()

print(f"\nWin Rate:        {wins/total*100:.1f}%")
print(f"Net P&L totale:  ${all_trades_B_valley['net_pnl'].sum():.2f}")
print(f"Avg trade P&L:   ${all_trades_B_valley['net_pnl'].mean():.2f}")
print(f"Profit Factor:   {all_trades_B_valley[all_trades_B_valley['net_pnl']>0]['net_pnl'].sum() / abs(all_trades_B_valley[all_trades_B_valley['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max Drawdown:    ${all_trades_B_valley['net_pnl'].cumsum().cummax().sub(all_trades_B_valley['net_pnl'].cumsum()).max():.2f}")

print(f"Trade totals:    {total}")
print(f"Winners:     {wins}  ({wins/total*100:.1f}%)")
print(f"Losers:      {losses}  ({losses/total*100:.1f}%)")
print(f"Break-even:      {break_even}")

df_losers = all_trades_B_valley[all_trades_B_valley["net_pnl"] < 0]
# Quanto tempo vivono i loser?
print(df_losers["ticks_in_trade"].describe())
# MAE dei loser — quanto vanno contro prima di stoppare?
print(df_losers["mae_ticks"].describe())
# MFE dei loser — hanno mai visto profitto prima di stoppare?
print(df_losers["mfe_ticks"].describe())

df_winners = all_trades_B_valley[all_trades_B_valley["net_pnl"] > 0]
print("--- WINNER MFE ---")
print(df_winners["mfe_ticks"].describe())
print("\n--- WINNER ora di entrata ---")
df_winners["entry_hour"] = pd.to_datetime(df_winners["entry_datetime"]).dt.hour
print(df_winners.groupby("entry_hour")["net_pnl"].agg(["count", "sum", "mean"]))
print("\n--- WINNER vs LOSER per ora ---")
all_trades_B_valley["entry_hour"] = pd.to_datetime(all_trades_B_valley["entry_datetime"]).dt.hour
print(all_trades_B_valley.groupby("entry_hour").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

df_losers = all_trades_B_valley[all_trades_B_valley["net_pnl"] < 0]
print(all_trades_B_valley[all_trades_B_valley["mfe_ticks"] > 20][
    ["entry_datetime", "side", "mfe_ticks", "mae_ticks", "ticks_in_trade", "net_pnl"]
])

# ── ANALISI VIX CONTANGO ─────────────────────────────────────
print("\n--- P&L per regime VIX (contango/backwardation) ---")
print(all_trades_B_valley.groupby("is_contango_prev").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

# ── ANALISI GEX REGIME ───────────────────────────────────────
print("\n--- P&L per regime GEX (long gamma / short gamma) ---")
print(all_trades_B_valley.groupby("is_long_gamma_prev").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

# ── ANALISI GEX VALORE ───────────────────────────────────────
print("\n--- P&L per quartile GEX ---")
all_trades_B_valley["gex_quartile"] = pd.qcut(
    all_trades_B_valley["gex_prev"],
    q=4,
    labels=["Q1_short_gamma", "Q2", "Q3", "Q4_long_gamma"]
)
print(all_trades_B_valley.groupby("gex_quartile").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

# ── ANALISI COMBINATA VIX + GEX ──────────────────────────────
print("\n--- P&L combinato VIX contango + GEX regime ---")
print(all_trades_B_valley.groupby(
    ["is_contango_prev", "is_long_gamma_prev"]
).agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
    avg_pnl=("net_pnl", "mean"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

df_b = all_trades_B_valley

print("=== ANALISI SENZA FILTRO ORA ===")
print(f"Trade totali: {len(df_b)}")
print(f"Winner: {(df_b['net_pnl'] > 0).sum()}")
print(f"Win Rate: {(df_b['net_pnl'] > 0).mean()*100:.1f}%")
print(f"Net P&L: ${df_b['net_pnl'].sum():.2f}")

print("\n--- prev_session_range: WINNER vs LOSER ---")
print("\nWINNER:")
print(df_b[df_b["net_pnl"] > 0]["prev_session_range"].describe())
print("\nLOSER:")
print(df_b[df_b["net_pnl"] < 0]["prev_session_range"].describe())

print("\n--- Volume trigger: WINNER vs LOSER ---")
print("\nWINNER:")
print(df_b[df_b["net_pnl"] > 0]["Volume"].describe())
print("\nLOSER:")
print(df_b[df_b["net_pnl"] < 0]["Volume"].describe())

# I winner hanno range sistematicamente più basso dei loser — mediana 31 vs 43.
# I winner hanno volume leggermente più alto — mediana 15 vs 11.5. 
# Non è un discriminatore forte ma suggerisce che bubble più grandi producono segnali leggermente migliori.

df_filtered = df_b[
    (df_b["prev_session_range"] <= 40) &
    (df_b["Volume"] >= 15)
]

wins = (df_filtered["net_pnl"] > 0).sum()
losses = (df_filtered["net_pnl"] < 0).sum()
total = len(df_filtered)

print(f"Trade totali: {total}")
print(f"Winner: {wins}  ({wins/total*100:.1f}%)")
print(f"Loser:  {losses}  ({losses/total*100:.1f}%)")
print(f"Net P&L: ${df_filtered['net_pnl'].sum():.2f}")
print(f"Avg trade P&L: ${df_filtered['net_pnl'].mean():.2f}")
print(f"Profit Factor: {df_filtered[df_filtered['net_pnl']>0]['net_pnl'].sum() / abs(df_filtered[df_filtered['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max Drawdown: ${df_filtered['net_pnl'].cumsum().cummax().sub(df_filtered['net_pnl'].cumsum()).max():.2f}")

print("\n--- Winner vs Loser per ora ---")
df_filtered["entry_hour"] = pd.to_datetime(df_filtered["entry_datetime"]).dt.hour
print(df_filtered.groupby("entry_hour").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

# Solo ore 11-12 senza altri filtri
df_11_12 = df_b[df_b["entry_hour"].isin([11, 12])]
wins = (df_11_12["net_pnl"] > 0).sum()
total = len(df_11_12)
print(f"Solo ore 11-12:")
print(f"Trade: {total}, Win Rate: {wins/total*100:.1f}%, P&L: ${df_11_12['net_pnl'].sum():.2f}")
print(f"PF: {df_11_12[df_11_12['net_pnl']>0]['net_pnl'].sum() / abs(df_11_12[df_11_12['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max DD: ${df_11_12['net_pnl'].cumsum().cummax().sub(df_11_12['net_pnl'].cumsum()).max():.2f}")

# Ore 11-12 + volume >= 15
df_11_12_vol = df_b[
    (df_b["entry_hour"].isin([11, 12])) &
    (df_b["Volume"] >= 15)
]
wins = (df_11_12_vol["net_pnl"] > 0).sum()
total = len(df_11_12_vol)
print(f"\nOre 11-12 + volume>=15:")
print(f"Trade: {total}, Win Rate: {wins/total*100:.1f}%, P&L: ${df_11_12_vol['net_pnl'].sum():.2f}")
print(f"PF: {df_11_12_vol[df_11_12_vol['net_pnl']>0]['net_pnl'].sum() / abs(df_11_12_vol[df_11_12_vol['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max DD: ${df_11_12_vol['net_pnl'].cumsum().cummax().sub(df_11_12_vol['net_pnl'].cumsum()).max():.2f}")

# Ore 11-12 + prev_session_range + volume
df_combined = df_b[
    (df_b["entry_hour"].isin([11, 12])) &
    (df_b["prev_session_range"] <= 40) &
    (df_b["Volume"] >= 15)
]
wins = (df_combined["net_pnl"] > 0).sum()
total = len(df_combined)
print(f"\nOre 11-12 + range<=40 + volume>=15:")
print(f"Trade: {total}, Win Rate: {wins/total*100:.1f}%, P&L: ${df_combined['net_pnl'].sum():.2f}")
print(f"PF: {df_combined[df_combined['net_pnl']>0]['net_pnl'].sum() / abs(df_combined[df_combined['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max DD: ${df_combined['net_pnl'].cumsum().cummax().sub(df_combined['net_pnl'].cumsum()).max():.2f}")

for name, df in [
    ("Ore 11-12", df_11_12),
    ("Ore 11-12 + Vol>=15", df_11_12_vol),
    ("Ore 11-12 + Vol>=15 + Range<=40", df_combined)
]:
    print(f"\n{'='*60}")
    print(f"{name} — {len(df)} trade")
    print(f"{'='*60}")

    print("\n--- VIX contango ---")
    print(df.groupby("is_contango_prev").agg(
        total=("net_pnl", "count"),
        winners=("net_pnl", lambda x: (x > 0).sum()),
        net_pnl=("net_pnl", "sum"),
        avg_pnl=("net_pnl", "mean"),
    ).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

    print("\n--- GEX regime ---")
    print(df.groupby("is_long_gamma_prev").agg(
        total=("net_pnl", "count"),
        winners=("net_pnl", lambda x: (x > 0).sum()),
        net_pnl=("net_pnl", "sum"),
        avg_pnl=("net_pnl", "mean"),
    ).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

    print("\n--- VIX + GEX combinato ---")
    print(df.groupby(["is_contango_prev", "is_long_gamma_prev"]).agg(
        total=("net_pnl", "count"),
        winners=("net_pnl", lambda x: (x > 0).sum()),
        net_pnl=("net_pnl", "sum"),
        avg_pnl=("net_pnl", "mean"),
    ).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

    # Quanti trade totali (senza filtro ora) cadono in contango?
print(df_b.groupby("is_contango_prev").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

df_gex_vol = df_b[
    (df_b["is_long_gamma_prev"] == False) &
    (df_b["Volume"] >= 15)
]

wins = (df_gex_vol["net_pnl"] > 0).sum()
losses = (df_gex_vol["net_pnl"] < 0).sum()
total = len(df_gex_vol)

print(f"Trade totali: {total}")
print(f"Winner: {wins}  ({wins/total*100:.1f}%)")
print(f"Loser:  {losses}  ({losses/total*100:.1f}%)")
print(f"Net P&L: ${df_gex_vol['net_pnl'].sum():.2f}")
print(f"Avg trade P&L: ${df_gex_vol['net_pnl'].mean():.2f}")
print(f"Profit Factor: {df_gex_vol[df_gex_vol['net_pnl']>0]['net_pnl'].sum() / abs(df_gex_vol[df_gex_vol['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max Drawdown: ${df_gex_vol['net_pnl'].cumsum().cummax().sub(df_gex_vol['net_pnl'].cumsum()).max():.2f}")

print("\n--- Winner vs Loser per ora ---")
print(df_gex_vol.groupby("entry_hour").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))

print("\n--- prev_session_range: WINNER vs LOSER ---")
print("\nWINNER:")
print(df_gex_vol[df_gex_vol["net_pnl"] > 0]["prev_session_range"].describe())
print("\nLOSER:")
print(df_gex_vol[df_gex_vol["net_pnl"] < 0]["prev_session_range"].describe())


# Tutte le combinazioni sistematiche
df_b = all_trades_B_valley.copy()

for ora in [None, [9], [9, 10], [9, 10, 11], [9, 10, 11, 12], [9, 10, 11, 12, 13], 
            [9, 10, 11, 12, 13, 14], [9, 10, 11, 12, 13, 14, 15],
            [10, 11, 12, 13, 14, 15], [12, 13, 14, 15], [13, 14, 15],
            [14, 15], [10, 11, 12, 13, 14], [10, 11, 12, 13,],
            [14], [9, 14]]:
    for gex in [None, False]:
        for vix in [None, False]:
            df_tmp = df_b.copy()
            label = []
            if ora:
                df_tmp = df_tmp[df_tmp["entry_hour"].isin(ora)]
                label.append(f"ore={ora}")
            if gex is not None:
                df_tmp = df_tmp[df_tmp["is_long_gamma_prev"] == gex]
                label.append(f"gex_short")
            if vix is not None:
                df_tmp = df_tmp[df_tmp["is_contango_prev"] == vix]
                label.append(f"backwardation")
            if len(df_tmp) < 8:
                continue
            wins = (df_tmp["net_pnl"] > 0).sum()
            total = len(df_tmp)
            pf = (df_tmp[df_tmp["net_pnl"]>0]["net_pnl"].sum() /
                  abs(df_tmp[df_tmp["net_pnl"]<0]["net_pnl"].sum())) if (df_tmp["net_pnl"]<0).any() else 999
            print(f"{' + '.join(label) or 'nessun filtro':55s} | trade={total:3d} | wr={wins/total*100:4.1f}% | pnl=${df_tmp['net_pnl'].sum():7.0f} | pf={pf:.2f}")
      
# -------------------------------- THIRD VARIANT --------------------------------
all_trades_B_valley_no_nodecvd = pd.concat([
    pd.read_parquet(f)
    for f in sorted(glob.glob(FOLDER + "trades_C_*.parquet"))
], ignore_index=True)

# re-calculate trade_id
all_trades_B_valley_no_nodecvd["trade_id"] = range(1, len(all_trades_B_valley_no_nodecvd) + 1)

print(f"Variant C")
print(f"Trades total: {len(all_trades_B_valley_no_nodecvd)}")
print(f"Period: {all_trades_B_valley_no_nodecvd['entry_datetime'].min()} → {all_trades_B_valley_no_nodecvd['entry_datetime'].max()}")

# Some measures aggregations
wins = (all_trades_B_valley_no_nodecvd["net_pnl"] > 0).sum()
losses = (all_trades_B_valley_no_nodecvd["net_pnl"] < 0).sum()
total = len(all_trades_B_valley_no_nodecvd)
break_even = (all_trades_B_valley_no_nodecvd["net_pnl"] == 0).sum()

print(f"\nWin Rate:        {wins/total*100:.1f}%")
print(f"Net P&L totale:  ${all_trades_B_valley_no_nodecvd['net_pnl'].sum():.2f}")
print(f"Avg trade P&L:   ${all_trades_B_valley_no_nodecvd['net_pnl'].mean():.2f}")
print(f"Profit Factor:   {all_trades_B_valley_no_nodecvd[all_trades_B_valley_no_nodecvd['net_pnl']>0]['net_pnl'].sum() / abs(all_trades_B_valley_no_nodecvd[all_trades_B_valley_no_nodecvd['net_pnl']<0]['net_pnl'].sum()):.2f}")
print(f"Max Drawdown:    ${all_trades_B_valley_no_nodecvd['net_pnl'].cumsum().cummax().sub(all_trades_B_valley_no_nodecvd['net_pnl'].cumsum()).max():.2f}")

print(f"Trade totals:    {total}")
print(f"Winners:     {wins}  ({wins/total*100:.1f}%)")
print(f"Losers:      {losses}  ({losses/total*100:.1f}%)")
print(f"Break-even:      {break_even}")

df_losers = all_trades_B_valley_no_nodecvd[all_trades_B_valley_no_nodecvd["net_pnl"] < 0]
# Quanto tempo vivono i loser?
print(df_losers["ticks_in_trade"].describe())
# MAE dei loser — quanto vanno contro prima di stoppare?
print(df_losers["mae_ticks"].describe())
# MFE dei loser — hanno mai visto profitto prima di stoppare?
print(df_losers["mfe_ticks"].describe())

# Conclusione strutturale: i trade perdenti sono sbagliati dalla radice — non sono trade buoni che poi si invertono. Il segnale è errato al momento dell'entry, non è un problema di gestione del trade.

df_winners = all_trades_B_valley_no_nodecvd[all_trades_B_valley_no_nodecvd["net_pnl"] > 0]
print("--- WINNER MFE ---")
print(df_winners["mfe_ticks"].describe())
print("\n--- WINNER ora di entrata ---")
df_winners["entry_hour"] = pd.to_datetime(df_winners["entry_datetime"]).dt.hour
print(df_winners.groupby("entry_hour")["net_pnl"].agg(["count", "sum", "mean"]))
print("\n--- WINNER vs LOSER per ora ---")
all_trades_B_valley_no_nodecvd["entry_hour"] = pd.to_datetime(all_trades_B_valley_no_nodecvd["entry_datetime"]).dt.hour
print(all_trades_B_valley_no_nodecvd.groupby("entry_hour").agg(
    total=("net_pnl", "count"),
    winners=("net_pnl", lambda x: (x > 0).sum()),
    net_pnl=("net_pnl", "sum"),
).assign(win_rate=lambda x: (x["winners"]/x["total"]*100).round(1)))
