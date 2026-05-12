"""
Mean Reversion analysis runner.

Loads all monthly trade parquets produced by
runner_data_valentini_meanreversion_backtest.py, joins VIX (contango regime)
and GEX (long/short gamma) regimes by trade date, then:

  Phase 1: aggregate KPI ranking across the 6 combos (3 locations × 2 targets)
  Phase 2: per-combo drilldown by hour, VIX regime, GEX regime, GEX quartile,
           and a TF-style nested hour × regime sweep.

No file is written — output is stdout only. Use the printed tables to
decide which combo and which regime filter to promote to a final strategy.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import polars as pl


pl.Config.set_tbl_rows(100)
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 40)


# region -------------------------------- CONFIG ---------------------------------------
FOLDER = r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/ES/Trades/Valentini/mirror_reverting/"

VIX_GLOB = r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/VixCentral/year=*/*.parquet"
GEX_PATH = r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/Sources/GEX/DIX.csv"

LOCATION_VARIANTS = ["A_LVN", "B_valley", "C_LVN_valley"]
TARGET_VARIANTS   = ["PrevPOC", "POC"]

# Declared in-scope months. Excludes 202601 backup files that may exist on disk.
# Extending the dataset → add months here AND extend the backtest runner MONTHS list.
MONTHS = [
    "202501", "202502", "202503", "202504", "202505", "202506",
    "202507", "202508", "202509", "202510", "202511", "202512",
]

# In-Sample / Out-of-Sample split. Tune filters on IS only; treat OOS as verdict.
# Default: 9 months IS (Jan-Sep 2025) / 3 months OOS (Oct-Dec 2025).
IS_MONTHS  = MONTHS[:9]
OOS_MONTHS = MONTHS[9:]
# endregion


# region -------------------------- VIX / GEX LOADERS ----------------------------------
def load_vix_lagged() -> pl.DataFrame:
    """Load VixCentral, shift by 1 day to avoid lookahead, expose contango regime."""
    return (
        pl.read_parquet(VIX_GLOB)
        .sort("date")
        .with_columns([
            pl.col("is_contango_f2_minus_f1").shift(1).alias("is_contango_prev"),
            pl.col("regime").shift(1).alias("regime_prev"),
            pl.col("contango_f2_minus_f1").shift(1).alias("contango_f2_minus_f1_prev"),
        ])
        .select(["date", "is_contango_prev", "regime_prev", "contango_f2_minus_f1_prev"])
    )


def load_gex_lagged() -> pl.DataFrame:
    """Load DIX/GEX, compute 252d MA on GEX, expose long/short gamma regime — all shifted by 1 day."""
    return (
        pl.read_csv(GEX_PATH, try_parse_dates=True)
        .sort("date")
        .with_columns([
            pl.col("gex").rolling_mean(252).alias("gex_ma252"),
            (pl.col("gex") > pl.col("gex").rolling_mean(252))
              .shift(1).alias("is_long_gamma_prev"),
            pl.col("dix").shift(1).alias("dix_prev"),
            pl.col("gex").shift(1).alias("gex_prev"),
        ])
        .select(["date", "is_long_gamma_prev", "dix_prev", "gex_prev"])
    )
# endregion


# region ------------------------- TRADE LOAD + JOIN -----------------------------------
def load_combo(variant: str, target: str, vix_df: pl.DataFrame, gex_df: pl.DataFrame) -> Optional[pd.DataFrame]:
    """Concat declared monthly files for a combo, recalc trade_id, join VIX+GEX, return Pandas.

    Files are picked explicitly from MONTHS — backup files outside the declared
    period (e.g. 202601 sanity-check files) are intentionally ignored.
    """
    parts = []
    missing = []
    for m in MONTHS:
        f = FOLDER + f"trades_MR_{variant}_{target}_{m}.parquet"
        if not Path(f).exists():
            missing.append(m)
            continue
        sub = pd.read_parquet(f)
        sub["source_month"] = m
        parts.append(sub)
    if missing:
        print(f"  [warn] MR_{variant}_{target}: missing months {missing}")
    if not parts:
        return None

    df = pd.concat(parts, ignore_index=True)
    if df.empty:
        return None
    df["trade_id"] = range(1, len(df) + 1)
    df["period"] = df["source_month"].apply(
        lambda m: "IS" if m in IS_MONTHS else ("OOS" if m in OOS_MONTHS else "EXCLUDED")
    )

    df = (
        pl.from_pandas(df)
        .with_columns(pl.col("entry_datetime").dt.date().alias("date"))
        .join(vix_df, on="date", how="left")
        .join(gex_df, on="date", how="left")
        .drop("date")
        .to_pandas()
    )
    df["entry_hour"] = pd.to_datetime(df["entry_datetime"]).dt.hour
    df["combo"] = f"MR_{variant}_{target}"
    return df
# endregion


# region ------------------------------- KPI -------------------------------------------
def compute_kpis(df: pd.DataFrame) -> dict:
    """Aggregate KPIs from a trade DataFrame (must contain net_pnl, side, mae_ticks, mfe_ticks)."""
    n = len(df)
    if n == 0:
        return {"n_trades": 0}

    wins   = (df["net_pnl"] > 0).sum()
    losses = (df["net_pnl"] < 0).sum()
    gross_profit = df.loc[df["net_pnl"] > 0, "net_pnl"].sum()
    gross_loss   = abs(df.loc[df["net_pnl"] < 0, "net_pnl"].sum())
    net          = df["net_pnl"].sum()
    pf           = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    cum = df["net_pnl"].cumsum()
    max_dd = (cum.cummax() - cum).max()

    longs  = df[df["side"] == "LONG"]
    shorts = df[df["side"] == "SHORT"]
    long_wr  = (longs["net_pnl"] > 0).mean() * 100 if len(longs) else float("nan")
    short_wr = (shorts["net_pnl"] > 0).mean() * 100 if len(shorts) else float("nan")

    return {
        "n_trades":   n,
        "win_rate":   wins / n * 100,
        "net":        net,
        "gross_p":    gross_profit,
        "gross_l":    gross_loss,
        "pf":         pf,
        "avg_pnl":    df["net_pnl"].mean(),
        "max_win":    df["net_pnl"].max(),
        "max_loss":   df["net_pnl"].min(),
        "max_dd":     max_dd,
        "n_long":     len(longs),
        "n_short":    len(shorts),
        "long_wr":    long_wr,
        "short_wr":   short_wr,
        "avg_mae":    df["mae_ticks"].mean(),
        "avg_mfe":    df["mfe_ticks"].mean(),
        "avg_ticks":  df["ticks_in_trade"].mean(),
    }


def fmt_pf(x: float) -> str:
    if pd.isna(x):
        return "nan"
    return "inf" if x == float("inf") else f"{x:.2f}"


def kpi_compact(df: pd.DataFrame) -> dict:
    """Lightweight KPI subset for IS/OOS side-by-side reporting."""
    n = len(df)
    if n == 0:
        return {"n": 0, "wr": float("nan"), "net": 0.0, "pf": float("nan")}
    wins = (df["net_pnl"] > 0).sum()
    gp   = df.loc[df["net_pnl"] > 0, "net_pnl"].sum()
    gl   = abs(df.loc[df["net_pnl"] < 0, "net_pnl"].sum())
    pf   = (gp / gl) if gl > 0 else float("inf")
    return {"n": n, "wr": wins / n * 100, "net": df["net_pnl"].sum(), "pf": pf}
# endregion


# region ------------------------ PHASE 1: COMBO RANKING -------------------------------
def phase1_ranking(combos: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Phase 1 ranking with IS / OOS / FULL columns side by side."""
    rows = []
    for combo_name, df in combos.items():
        k_full = kpi_compact(df)
        k_is   = kpi_compact(df[df["period"] == "IS"])
        k_oos  = kpi_compact(df[df["period"] == "OOS"])
        rows.append({
            "combo":      combo_name,
            "IS_n":       k_is["n"],
            "IS_wr_%":    round(k_is["wr"], 1) if pd.notna(k_is["wr"]) else None,
            "IS_pf":      fmt_pf(k_is["pf"]),
            "IS_net_$":   round(k_is["net"], 0),
            "OOS_n":      k_oos["n"],
            "OOS_wr_%":   round(k_oos["wr"], 1) if pd.notna(k_oos["wr"]) else None,
            "OOS_pf":     fmt_pf(k_oos["pf"]),
            "OOS_net_$":  round(k_oos["net"], 0),
            "FULL_n":     k_full["n"],
            "FULL_wr_%":  round(k_full["wr"], 1) if pd.notna(k_full["wr"]) else None,
            "FULL_pf":    fmt_pf(k_full["pf"]),
            "FULL_net_$": round(k_full["net"], 0),
        })
    return pd.DataFrame(rows).sort_values("IS_net_$", ascending=False).reset_index(drop=True)
# endregion


# region ------------------------ PHASE 2: DRILLDOWN -----------------------------------
def slice_kpis_is_oos(df: pd.DataFrame, group_col) -> pd.DataFrame:
    """Group by `group_col`, compute KPIs separately for IS / OOS / FULL.

    Output columns: IS_n, IS_pf, IS_net, OOS_n, OOS_pf, OOS_net, FULL_n, FULL_pf, FULL_net.
    """
    rows = []
    keys = df.groupby(group_col, dropna=False).groups.keys()
    for key in keys:
        if isinstance(group_col, list):
            mask = pd.Series(True, index=df.index)
            for col, val in zip(group_col, key if isinstance(key, tuple) else (key,)):
                if pd.isna(val):
                    mask &= df[col].isna()
                else:
                    mask &= df[col] == val
        else:
            mask = df[group_col].isna() if pd.isna(key) else (df[group_col] == key)
        sub = df[mask]
        k_is   = kpi_compact(sub[sub["period"] == "IS"])
        k_oos  = kpi_compact(sub[sub["period"] == "OOS"])
        k_full = kpi_compact(sub)
        rows.append({
            (group_col if isinstance(group_col, str) else "_".join(group_col)): key,
            "IS_n":       k_is["n"],
            "IS_pf":      fmt_pf(k_is["pf"]),
            "IS_net":     round(k_is["net"], 0),
            "OOS_n":      k_oos["n"],
            "OOS_pf":     fmt_pf(k_oos["pf"]),
            "OOS_net":    round(k_oos["net"], 0),
            "FULL_n":     k_full["n"],
            "FULL_pf":    fmt_pf(k_full["pf"]),
            "FULL_net":   round(k_full["net"], 0),
        })
    return pd.DataFrame(rows)


def drilldown_combo(combo_name: str, df: pd.DataFrame) -> None:
    print(f"\n{'#' * 70}")
    print(f"#  DRILLDOWN  {combo_name}   (n={len(df)}  IS={int((df['period']=='IS').sum())}  OOS={int((df['period']=='OOS').sum())})")
    print(f"{'#' * 70}")

    if len(df) == 0:
        print("  No trades.")
        return

    print("\n--- By entry_hour ---")
    print(slice_kpis_is_oos(df, "entry_hour").to_string(index=False))

    print("\n--- By VIX regime (is_contango_prev) ---")
    print(slice_kpis_is_oos(df, "is_contango_prev").to_string(index=False))

    print("\n--- By GEX regime (is_long_gamma_prev) ---")
    print(slice_kpis_is_oos(df, "is_long_gamma_prev").to_string(index=False))

    if df["gex_prev"].notna().sum() >= 8:
        df = df.copy()
        df["gex_quartile"] = pd.qcut(
            df["gex_prev"], q=4,
            labels=["Q1_short_gamma", "Q2", "Q3", "Q4_long_gamma"],
            duplicates="drop",
        )
        print("\n--- By GEX quartile ---")
        print(slice_kpis_is_oos(df, "gex_quartile").to_string(index=False))

    print("\n--- Combined VIX × GEX ---")
    print(slice_kpis_is_oos(df, ["is_contango_prev", "is_long_gamma_prev"]).to_string(index=False))

    print("\n--- Hour-window × regime sweep (IS_n>=15, OOS shown for any IS-passing combo) ---")
    print("  EXPLORATORY — do not pick a filter from this table without a causal hypothesis. Multiple-testing risk is uncontrolled.")
    hour_windows = [
        None,
        [9], [9, 10], [9, 10, 11], [9, 10, 11, 12],
        [9, 10, 11, 12, 13], [9, 10, 11, 12, 13, 14],
        [10, 11, 12, 13, 14, 15], [11, 12], [11, 12, 13],
        [12, 13, 14], [13, 14], [14, 15], [10, 11], [10, 11, 12],
        [10],
    ]
    for hours in hour_windows:
        for gex in [None, True, False]:
            for vix in [None, True, False]:
                tmp = df
                label = []
                if hours is not None:
                    tmp = tmp[tmp["entry_hour"].isin(hours)]
                    label.append(f"hours={hours}")
                if gex is not None:
                    tmp = tmp[tmp["is_long_gamma_prev"] == gex]
                    label.append("gex_long" if gex else "gex_short")
                if vix is not None:
                    tmp = tmp[tmp["is_contango_prev"] == vix]
                    label.append("contango" if vix else "backwardation")
                k_is  = kpi_compact(tmp[tmp["period"] == "IS"])
                k_oos = kpi_compact(tmp[tmp["period"] == "OOS"])
                if k_is["n"] < 15:
                    continue
                oos_str = (
                    f"OOS n={k_oos['n']:3d} pf={fmt_pf(k_oos['pf']):>5s} "
                    f"net=${k_oos['net']:7.0f}"
                    if k_oos["n"] > 0 else "OOS n=  0  (no out-of-sample trades)"
                )
                print(
                    f"  {' + '.join(label) or 'no_filter':55s} | "
                    f"IS n={k_is['n']:3d} pf={fmt_pf(k_is['pf']):>5s} net=${k_is['net']:7.0f} | "
                    f"{oos_str}"
                )
# endregion


# region -------------------------------- MAIN -----------------------------------------
def main() -> None:
    print("Loading VIX + GEX...")
    vix_df = load_vix_lagged()
    gex_df = load_gex_lagged()

    print("Loading combos...")
    combos: dict[str, pd.DataFrame] = {}
    for v in LOCATION_VARIANTS:
        for t in TARGET_VARIANTS:
            df = load_combo(v, t, vix_df, gex_df)
            if df is None or df.empty:
                print(f"  [skip] MR_{v}_{t}: no files / empty")
                continue
            combos[f"MR_{v}_{t}"] = df
            print(f"  MR_{v}_{t}: {len(df)} trades  "
                  f"({df['entry_datetime'].min().date()} → {df['entry_datetime'].max().date()})")

    if not combos:
        print("No combos loaded. Abort.")
        return

    # ---------------- Phase 1 ----------------
    print(f"\n{'=' * 70}")
    print("  PHASE 1 — COMBO RANKING (IS / OOS / FULL)")
    print(f"  IS  = {IS_MONTHS}")
    print(f"  OOS = {OOS_MONTHS}")
    print(f"  Rule: tune filters on IS only; OOS is verdict, not training.")
    print(f"{'=' * 70}")
    ranking = phase1_ranking(combos)
    print(ranking.to_string(index=False))

    # ---------------- Phase 2 ----------------
    print(f"\n{'=' * 70}")
    print("  PHASE 2 — PER-COMBO DRILLDOWN")
    print(f"{'=' * 70}")
    for combo_name in ranking["combo"]:
        drilldown_combo(combo_name, combos[combo_name])

    print(f"\n{'=' * 70}")
    print("Analysis complete.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
# endregion
