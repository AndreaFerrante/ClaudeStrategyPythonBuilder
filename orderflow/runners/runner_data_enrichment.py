"""
Monthly tick-data enrichment runner.

Re-architected from imperative script into resumable, checkpointed pipeline:

  S1: Load weekly .txt files from FOLDER, add Hour and SessionType.
  S2: Add POC and Prev_POC (pandas roundtrip).
  S3: Add VA_Areas and ValleysPeaks (pandas roundtrip).
  S4: Add CD_Ask/CD_Bid/CD_Total, Session_High/Low, Node_*_Volume, LVN, Index.
  S5: Compress to 1-minute bars; join current_bar_* and next_bar_* onto ticks.
  S6: Aggregate auctions and detect blocks; attach block info to each tick.

Each stage writes a parquet checkpoint under CHECKPOINT_DIR with a static name
(s1.parquet .. s6.parquet). On re-run, stages whose checkpoint already exists
are skipped and their output is fed forward to the next missing stage. The
final parquet is written atomically via tmp + Path.replace and named
`YYYYMMDD_to_YYYYMMDD_ES.parquet` from the min/max Datetime found in the data;
intermediate checkpoints are then deleted unless KEEP_CHECKPOINTS is True.

Operator workflow
-----------------
1. Stage the weekly .txt files into FOLDER (one batch per run, typically the
   weeks of a single month).
2. Run with optional positional symbol (default: TICKER on line 74):
     python -m orderflow.runners.runner_data_enrichment        # uses TICKER
     python -m orderflow.runners.runner_data_enrichment ES
     python -m orderflow.runners.runner_data_enrichment ZN
     python -m orderflow.runners.runner_data_enrichment MES
   Paths (unarchive/, checkpoints/, parquet/) are resolved automatically under
   sources/<SYMBOL>/. The symbol must exist in cf.FUTURE_VALUES (Tick_Size lookup).
3. Optionally rename the output parquet to a month label (e.g.
   `202503_ES.parquet`).

On crash mid-run, simply re-launch: the pipeline resumes from the last
completed stage. The mtime of every pre-existing checkpoint is logged at the
start of each run so stale leftovers from prior batches are visible.
"""

from __future__ import annotations

import argparse
import gc
import logging
import logging.handlers
import time as _time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd
import polars as pl

try:
    import psutil  # type: ignore
except ImportError:  # diagnostic logging only; runner works without it
    psutil = None  # type: ignore

import orderflow.configuration as cf
from orderflow._volume_factory import (
    get_market_evening_session,
    get_tickers_in_folder_mem_optim,
)
from orderflow.auctions import aggregate_auctions, get_valid_blocks
from orderflow.compressor import compress_to_minute_bars_pl
from orderflow.volume_profile import (
    get_daily_high_and_low_by_session,
    get_daily_session_moving_POC,
    get_dynamic_cumulative_delta_per_session,
    get_volume_profile_areas,
    get_volume_profile_node_volume,
    get_volume_profile_peaks_valleys,
)

# region --------------------------------- CONFIG ---------------------------------------

# No MONTH constant: output filename is derived from min/max Datetime found in
# the loaded weekly .txt files (see `_derive_final_name`). Operator stages weekly
# .txt files into FOLDER and runs the script; output goes to FINAL_DIR.

TICKER: str = "ES"
MARKET: str = "CME"
SEPARATOR: str = ";"
EXTENSION: str = ".txt"

ROOT: Path = Path(
    r"C:/Users/Tommy/Documents/PycharmProjects/Orderflow/sources/"
)
FOLDER: Path = Path(
    Path(ROOT) / "ES/unarchive/"
)
CHECKPOINT_DIR: Path = Path(
    Path(ROOT) / "ES/checkpoints/"
)
FINAL_DIR: Path = Path(
    Path(ROOT) / "ES/parquet/"
)
LOG_DIR: Path = Path("./log/")

# Auction / block detection parameters (unchanged from the original imperative script).
N_CONSECUTIVE: int = 2
VOL_THRESH: int = 100
MIN_ABS_IMB: float = 1.5
IMBALANCE_MODE: str = "ratio"

# Retain s1..s5 parquets on disk after final write? Set True when debugging stages.
KEEP_CHECKPOINTS: bool = False

# LVN volume-fraction threshold (matches original script: 0.25 * avg-volume-per-tick).
LVN_VOLUME_FRACTION: float = 0.25

# Polars display configuration (matches original).
pl.Config.set_tbl_rows(100)

TICK_SIZE: float = cf.FUTURE_VALUES.loc[
    cf.FUTURE_VALUES["Ticker"] == TICKER, "Tick_Size"
].values[0]

# endregion


# region --------------------------- LOGGING / UTILITIES --------------------------------

logger = logging.getLogger("runner_data_enrichment")


def _configure_logger() -> None:
    """Configure rotating file + stderr handlers. Idempotent."""
    if logger.handlers:
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    file_handler = logging.handlers.RotatingFileHandler(
        filename=LOG_DIR / "runner_data_enrichment.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False


def _rss_gb() -> float:
    """Current process resident set size, in GB. Returns 0.0 if psutil missing."""
    if psutil is None:
        return 0.0
    return psutil.Process().memory_info().rss / 1e9


def _assert_columns(df: pl.DataFrame, expected: Iterable[str], stage: str) -> None:
    """Hard-fail if any expected column is missing from a checkpoint."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise AssertionError(
            f"[{stage}] checkpoint is missing required columns: {missing}"
        )


def _run_stage(
    stage_name: str,
    out_path: Path,
    stage_fn: Callable[[], pl.DataFrame],
) -> Path:
    """
    Execute a stage if its output parquet is not yet on disk.

    The stage callable returns the freshly computed Polars DataFrame; this helper
    handles checkpoint detection, write, logging and post-stage GC.
    """
    if out_path.exists():
        logger.info("[%s] checkpoint exists, skipping (%s)", stage_name, out_path.name)
        return out_path

    logger.info("[%s] start | rss=%.2f GB", stage_name, _rss_gb())
    t0 = _time.perf_counter()
    df = stage_fn()
    # Atomic-ish write: stages are intermediate; replace() is only required for final.
    df.write_parquet(out_path, compression="snappy")
    rows = df.height
    del df
    gc.collect()
    duration = _time.perf_counter() - t0
    logger.info(
        "[%s] done  | rows=%d | duration=%.1fs | peak_rss=%.2f GB | out=%s",
        stage_name,
        rows,
        duration,
        _rss_gb(),
        out_path.name,
    )
    return out_path


# endregion


# region ----------------------------- STAGE FUNCTIONS ----------------------------------


def stage_1_load_and_session(_prev: Path | None) -> pl.DataFrame:
    """
    S1 — Load weekly .txt files from FOLDER, concatenate into one DataFrame,
    add Hour and SessionType columns. Logs the min/max Datetime so the operator
    can verify the batch span before stages 2-6 run.
    """
    df = get_tickers_in_folder_mem_optim(
        path=str(FOLDER) + "/",
        ticker=TICKER,
        extension=EXTENSION,
        separator=SEPARATOR,
        market=MARKET,
    )
    df = df.with_columns(Hour=pl.col("Datetime").dt.hour())
    df = df.with_columns(SessionType=get_market_evening_session(data=df, ticker=TICKER))
    dt_min = df.select(pl.col("Datetime").min()).item()
    dt_max = df.select(pl.col("Datetime").max()).item()
    logger.info("[S1] batch span: %s -> %s", dt_min, dt_max)
    return df


def stage_2_poc(prev: Path) -> pl.DataFrame:
    """
    S2 — Add POC and Prev_POC. Uses a pandas roundtrip on the minimal subset of
    columns needed by `get_daily_session_moving_POC` so the pandas frame is
    short-lived and scoped strictly inside this function.
    """
    df = pl.read_parquet(prev)
    _assert_columns(df, ["Datetime", "Price", "Volume", "SessionType"], "S2")

    cols_needed = ["Datetime", "Price", "Volume", "SessionType"]
    df_pd = df.select(cols_needed).to_pandas()
    # Force pure-Python datetime objects (matches original behaviour).
    df_pd["Datetime"] = pd.Series(
        df_pd["Datetime"].dt.to_pydatetime(), dtype=object
    )
    poc, prev_poc = get_daily_session_moving_POC(df_pd)
    del df_pd
    gc.collect()

    df = df.with_columns(
        [
            pl.Series("POC", poc),
            pl.Series("Prev_POC", prev_poc),
        ]
    )
    del poc, prev_poc
    gc.collect()
    return df


def stage_3_va_and_kde(prev: Path) -> pl.DataFrame:
    """
    S3 — Add VA_Areas (value-area label) and ValleysPeaks (KDE shape).
    Both derived from a single pandas subset.
    """
    df = pl.read_parquet(prev)
    _assert_columns(df, ["Price", "Volume", "SessionType", "POC", "Prev_POC"], "S3")

    cols_needed = ["Price", "Volume", "SessionType"]
    df_pd = df.select(cols_needed).to_pandas()
    va_areas = get_volume_profile_areas(df_pd)
    valleys_peaks = get_volume_profile_peaks_valleys(df_pd)
    del df_pd
    gc.collect()

    df = df.with_columns(
        [
            pl.Series("VA_Areas", va_areas),
            pl.Series("ValleysPeaks", valleys_peaks),
        ]
    )
    del va_areas, valleys_peaks
    gc.collect()
    return df


def stage_4_cd_lvn_index(prev: Path) -> pl.DataFrame:
    """
    S4 — Cumulative delta per session, session high/low, node volumes,
    LVN flag, and the global Index column required by the backtest engine.
    """
    df = pl.read_parquet(prev)
    _assert_columns(
        df,
        ["Price", "Volume", "TradeType", "SessionType", "POC", "VA_Areas"],
        "S4",
    )

    cols_needed = ["Price", "Volume", "TradeType", "SessionType"]
    df_pd = df.select(cols_needed).to_pandas()

    df_cd = get_dynamic_cumulative_delta_per_session(df_pd)
    cd_ask = df_cd["CD_Ask"].values
    cd_bid = df_cd["CD_Bid"].values
    cd_total = df_cd["CD_Total"].values
    del df_cd
    gc.collect()

    lows, highs = get_daily_high_and_low_by_session(df_pd)

    (
        price_tot_volume,
        price_askvolume,
        price_bidvolume,
        total_volumes,
    ) = get_volume_profile_node_volume(df_pd)
    del df_pd
    gc.collect()

    df = df.with_columns(
        [
            pl.Series("CD_Ask", cd_ask),
            pl.Series("CD_Bid", cd_bid),
            pl.Series("CD_Total", cd_total),
            pl.Series("Session_High", highs),
            pl.Series("Session_Low", lows),
            pl.Series("Node_Volume", price_tot_volume),
            pl.Series("Node_Ask_Volume", price_askvolume),
            pl.Series("Node_Bid_Volume", price_bidvolume),
            pl.Series("Session_Volume", total_volumes),
        ]
    )
    del cd_ask, cd_bid, cd_total, lows, highs
    del price_tot_volume, price_askvolume, price_bidvolume, total_volumes
    gc.collect()

    # LVN flag — Node_Volume below 25% of session-average per-tick volume.
    df = df.with_columns(
        (
            pl.col("Node_Volume")
            < LVN_VOLUME_FRACTION
            * (
                pl.col("Session_Volume")
                / ((pl.col("Session_High") - pl.col("Session_Low")) / TICK_SIZE)
            )
        )
        .cast(pl.Int8)
        .alias("LVN")
    )
    # Sequential global Index — required by the backtest engine.
    df = df.with_columns(Index=pl.int_range(0, pl.len()).alias("Index"))
    return df


def stage_5_minute_bars(prev: Path) -> pl.DataFrame:
    """
    S5 — Compress ticks to 1-minute bars and attach current_bar_* + next_bar_*
    columns to every tick via a backward join_asof on Datetime.
    """
    df = pl.read_parquet(prev)
    _assert_columns(df, ["Datetime", "Index", "LVN", "CD_Bid", "CD_Ask"], "S5")

    df_bars = compress_to_minute_bars_pl(
        df, win_compression="1m", time_column="Datetime"
    )
    df_bars = df_bars.with_columns(pl.col("Datetime").dt.replace_time_zone(None))
    df_bars = df_bars.with_columns(pl.col("Datetime").alias("current_bar_datetime"))

    rename_map = {
        col: f"current_bar_{col.lower()}"
        for col in df_bars.columns
        if col not in ("Datetime", "current_bar_datetime")
    }
    df_bars = df_bars.rename(rename_map)

    df_bars = df_bars.with_columns(
        [
            pl.col("Datetime").shift(-1).alias("next_bar_datetime"),
            pl.col("current_bar_open").shift(-1).alias("next_bar_open"),
            pl.col("current_bar_high").shift(-1).alias("next_bar_high"),
            pl.col("current_bar_low").shift(-1).alias("next_bar_low"),
            pl.col("current_bar_close").shift(-1).alias("next_bar_close"),
            pl.col("current_bar_volume").shift(-1).alias("next_bar_volume"),
            pl.col("current_bar_numberoftrades").shift(-1).alias("next_bar_num_trades"),
            pl.col("current_bar_askvolume").shift(-1).alias("next_bar_ask_volume"),
            pl.col("current_bar_bidvolume").shift(-1).alias("next_bar_bid_volume"),
        ]
    )

    df = df.join_asof(
        df_bars,
        left_on="Datetime",
        right_on="Datetime",
        strategy="backward",
    )
    del df_bars
    gc.collect()
    return df


def stage_6_auctions_and_blocks(prev: Path) -> pl.DataFrame:
    """
    S6 — Compute auctions and consecutive-imbalance blocks, then attach
    block-level info to each tick.

    The original script produced `df_agg` and `df_blocks` but never merged
    them back onto the tick DataFrame; the commented experiment showed the
    intended target was a `join_asof` against `EndTime`. We implement the
    minimal version of that join: each tick inherits the most recent block
    that ended at or before its timestamp, prefixed `block_*`.
    """
    df = pl.read_parquet(prev)
    _assert_columns(
        df,
        ["Datetime", "BidPrice", "AskPrice", "TradeType", "Volume", "Index"],
        "S6",
    )

    df_agg = aggregate_auctions(
        df=df,
        imbalance_mode=IMBALANCE_MODE,
    ).with_columns(
        pl.when(pl.col("BuyVolume") > pl.col("SellVolume"))
        .then(pl.lit("Long"))
        .otherwise(pl.lit("Short"))
        .alias("TradeSide")
    )
    logger.info("[S6] auctions shape=%s", df_agg.shape)

    df_blocks = get_valid_blocks(
        agg=df_agg,
        n_consecutive=N_CONSECUTIVE,
        vol_thresh=VOL_THRESH,
        min_abs_imbalance=MIN_ABS_IMB,
    )
    logger.info("[S6] blocks shape=%s", df_blocks.shape)
    del df_agg
    gc.collect()

    # Select a compact, block-level projection and prefix every column with
    # `block_` so the merge never collides with existing tick columns. The
    # `EndTime` field is kept under its prefixed name and used as the join key.
    if df_blocks.height > 0:
        block_cols_keep = [
            c
            for c in (
                "StartTime",
                "EndTime",
                "BlockId",
                "TotalBlockVolume",
                "TotalBlockImbalance",
                "ImbalanceDirection",
            )
            if c in df_blocks.columns
        ]
        df_blocks_small = df_blocks.select(block_cols_keep).rename(
            {c: f"block_{c.lower()}" for c in block_cols_keep}
        )
        df_blocks_small = df_blocks_small.sort("block_endtime")
        df = df.sort("Datetime").join_asof(
            df_blocks_small,
            left_on="Datetime",
            right_on="block_endtime",
            strategy="backward",
        )
        del df_blocks_small
    else:
        # Empty blocks DF — attach null columns so downstream code has a stable schema.
        df = df.with_columns(
            [
                pl.lit(None, dtype=pl.Datetime).alias("block_starttime"),
                pl.lit(None, dtype=pl.Datetime).alias("block_endtime"),
                pl.lit(None, dtype=pl.Int64).alias("block_blockid"),
                pl.lit(None, dtype=pl.Float64).alias("block_totalblockvolume"),
                pl.lit(None, dtype=pl.Float64).alias("block_totalblockimbalance"),
                pl.lit(None, dtype=pl.Int32).alias("block_imbalancedirection"),
            ]
        )

    del df_blocks
    gc.collect()
    return df


# endregion


# region -------------------------------- ORCHESTRATION ---------------------------------


def _checkpoint_path(stage_idx: int) -> Path:
    return CHECKPOINT_DIR / f"s{stage_idx}.parquet"


def _derive_final_name(s6_path: Path) -> str:
    """
    Derive the output parquet name from the min/max Datetime in the S6
    checkpoint: `YYYYMMDD_to_YYYYMMDD_ES.parquet`. The operator renames the file
    to a month label (e.g. 202503_ES.parquet) after the run if desired.
    """
    df_dt = pl.read_parquet(s6_path, columns=["Datetime"])
    dt_min = df_dt.select(pl.col("Datetime").min()).item()
    dt_max = df_dt.select(pl.col("Datetime").max()).item()
    del df_dt
    gc.collect()
    return f"{dt_min:%Y%m%d}_to_{dt_max:%Y%m%d}_{TICKER}.parquet"


def _atomic_finalize(last_checkpoint: Path) -> Path:
    """
    Promote the S6 checkpoint to the final parquet via tmp + replace. Filename
    is derived from the Datetime span found in the S6 checkpoint. Re-reads then
    rewrites to guarantee independence from the s6 file; intermediate
    checkpoints are then deleted unless KEEP_CHECKPOINTS.
    """
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    final_name = _derive_final_name(last_checkpoint)
    final = FINAL_DIR / final_name
    tmp = final.with_suffix(final.suffix + ".tmp")

    df = pl.read_parquet(last_checkpoint)
    df.write_parquet(tmp, compression="snappy")
    del df
    gc.collect()
    tmp.replace(final)
    return final


def _cleanup_checkpoints() -> None:
    if KEEP_CHECKPOINTS:
        logger.info("KEEP_CHECKPOINTS=True; preserving intermediate parquets")
        return
    for i in range(1, 7):
        p = _checkpoint_path(i)
        if p.exists():
            try:
                p.unlink()
                logger.info("deleted checkpoint %s", p.name)
            except OSError as exc:
                logger.warning("could not delete %s: %s", p.name, exc)


def process(symbol: str | None = None) -> Path:
    """
    Run the full enrichment pipeline on the .txt files currently staged in
    FOLDER. Resumes from the most advanced existing checkpoint. Returns the
    final parquet path. Output name is derived from the Datetime span found in
    the data (`YYYYMMDD_to_YYYYMMDD_{TICKER}.parquet`).

    Args:
        symbol: Instrument symbol override (e.g. "ZN", "MES"). If None, uses
            the module-level TICKER constant. Paths are derived automatically
            under sources/<symbol>/.
    """
    global TICKER, FOLDER, CHECKPOINT_DIR, FINAL_DIR, TICK_SIZE
    if symbol is not None:
        TICKER = symbol.upper()
    _sources_base = Path(ROOT) / TICKER
    FOLDER = _sources_base / "unarchive"
    CHECKPOINT_DIR = _sources_base / "checkpoints"
    FINAL_DIR = _sources_base / "parquet"
    TICK_SIZE = cf.FUTURE_VALUES.loc[
        cf.FUTURE_VALUES["Ticker"] == TICKER, "Tick_Size"
    ].values[0]

    _configure_logger()
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    pipeline: list[tuple[str, Callable[[Path | None], pl.DataFrame]]] = [
        ("S1", stage_1_load_and_session),
        ("S2", stage_2_poc),
        ("S3", stage_3_va_and_kde),
        ("S4", stage_4_cd_lvn_index),
        ("S5", stage_5_minute_bars),
        #("S6", stage_6_auctions_and_blocks),
    ]

    logger.info(
        "pipeline start | folder=%s | started_at=%s",
        FOLDER,
        datetime.now().isoformat(timespec="seconds"),
    )

    # Log mtime of any pre-existing checkpoints so the operator can spot stale ones.
    for idx in range(1, 7):
        cp = _checkpoint_path(idx)
        if cp.exists():
            mtime = datetime.fromtimestamp(cp.stat().st_mtime).isoformat(timespec="seconds")
            logger.info("resume: found %s (mtime=%s)", cp.name, mtime)

    prev_path: Path | None = None
    for idx, (name, fn) in enumerate(pipeline, start=1):
        out_path = _checkpoint_path(idx)
        # Closures capture prev_path by reference; bind explicitly via default arg.
        def _runner(_prev: Path | None = prev_path, _fn: Callable = fn) -> pl.DataFrame:
            return _fn(_prev)

        _run_stage(name, out_path, _runner)
        prev_path = out_path
        gc.collect()

    if prev_path is None:
        raise RuntimeError("pipeline produced no checkpoints")

    logger.info("finalizing from %s", prev_path.name)
    final_path = _atomic_finalize(prev_path)
    logger.info("final written: %s (rss=%.2f GB)", final_path, _rss_gb())
    _cleanup_checkpoints()
    logger.info("pipeline complete")
    return final_path


# endregion


if __name__ == "__main__":
    _parser = argparse.ArgumentParser(description="Tick-data enrichment runner")
    _parser.add_argument(
        "symbol",
        nargs="?",
        default=None,
        help="Instrument symbol, e.g. ES, ZN, MES (default: TICKER on line 74)",
    )
    _args = _parser.parse_args()
    print(f"[runner_data_enrichment] symbol={_args.symbol or TICKER}")
    process(symbol=_args.symbol)
