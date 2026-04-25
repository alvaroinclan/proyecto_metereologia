"""
Weibull distribution fitting and seasonal variability analysis.

This module provides functions for fitting two-parameter Weibull
distributions to wind speed data by station (and optionally by season),
and for analysing the seasonal variability of the fitted parameters.

The Weibull distribution is the standard model in wind energy for
characterising wind speed frequency distributions.  Its probability
density function (PDF) is:

    f(v) = (k / A) · (v / A)^(k-1) · exp(-(v / A)^k)

where *k* (shape) describes the breadth of the distribution and *A*
(scale) is related to the mean wind speed.

Fitting is performed via **Maximum Likelihood Estimation (MLE)** using
``scipy.stats.weibull_min``.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import weibull_min

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEASON_MAP: dict[int, str] = {
    12: "DJF",
    1: "DJF",
    2: "DJF",
    3: "MAM",
    4: "MAM",
    5: "MAM",
    6: "JJA",
    7: "JJA",
    8: "JJA",
    9: "SON",
    10: "SON",
    11: "SON",
}
"""Mapping from month number (1–12) to meteorological season code."""

SEASONS_ORDER = ["DJF", "MAM", "JJA", "SON"]
"""Canonical order for seasons (winter → spring → summer → autumn)."""

# Minimum number of valid (non-null, > 0) observations required to
# attempt a Weibull fit.  With fewer points the MLE is unreliable.
MIN_OBS_FOR_FIT = 10


# ---------------------------------------------------------------------------
# 1. Weibull fitting helpers
# ---------------------------------------------------------------------------


def fit_weibull(speeds: np.ndarray) -> tuple[float, float] | None:
    """Fit a two-parameter Weibull distribution via MLE.

    Parameters
    ----------
    speeds:
        1-D array of positive wind speeds (m/s).  Values ≤ 0 and NaNs
        should be removed **before** calling this function.

    Returns
    -------
    tuple[float, float] | None
        ``(shape_k, scale_A)`` if the fit succeeds, ``None`` if there
        are too few observations (< ``MIN_OBS_FOR_FIT``) or the
        optimiser fails.
    """
    if len(speeds) < MIN_OBS_FOR_FIT:
        return None

    try:
        # scipy convention: weibull_min.fit returns (c, loc, scale)
        # where c = shape (k) and scale = A.  We fix loc = 0 because
        # the two-parameter Weibull has no location shift.
        # method="MLE" → Maximum Likelihood Estimation (Reto Big Data).
        shape_k, _loc, scale_A = weibull_min.fit(speeds, floc=0, method="MLE")
        return float(shape_k), float(scale_A)
    except Exception:  # noqa: BLE001 – catch any scipy convergence error
        return None


# ---------------------------------------------------------------------------
# 2. Station-level fitting
# ---------------------------------------------------------------------------


def fit_weibull_by_station(
    df: pl.DataFrame,
    ws_col: str = "ws10",
    station_col: str = "station",
) -> pl.DataFrame:
    """Fit a Weibull distribution to each station's wind speeds.

    Calm and null observations are excluded before fitting.

    Parameters
    ----------
    df:
        Input DataFrame with at least *ws_col* and *station_col*.
    ws_col:
        Name of the wind-speed column (m/s).
    station_col:
        Name of the station identifier column.

    Returns
    -------
    pl.DataFrame
        Columns: *station_col*, ``weibull_k``, ``weibull_A``,
        ``mean_ws``, ``std_ws``, ``n_obs``.

    Raises
    ------
    ValueError
        If *ws_col* or *station_col* are not present in the DataFrame.
    """
    for col in (ws_col, station_col):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")

    records: list[dict] = []

    for station_name in sorted(df[station_col].unique().to_list()):
        station_speeds = (
            df.filter(
                (pl.col(station_col) == station_name)
                & pl.col(ws_col).is_not_null()
                & (pl.col(ws_col) > 0)
            )[ws_col]
            .to_numpy()
            .astype(np.float64)
        )

        result = fit_weibull(station_speeds)

        records.append(
            {
                station_col: station_name,
                "weibull_k": result[0] if result else None,
                "weibull_A": result[1] if result else None,
                "mean_ws": float(np.mean(station_speeds))
                if len(station_speeds) > 0
                else None,
                "std_ws": float(np.std(station_speeds))
                if len(station_speeds) > 0
                else None,
                "n_obs": len(station_speeds),
            }
        )

    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# 3. Seasonal fitting
# ---------------------------------------------------------------------------


def add_season_column(
    df: pl.DataFrame,
    time_col: str = "time",
) -> pl.DataFrame:
    """Add a ``season`` column based on the month of *time_col*.

    Uses the meteorological season convention:
    DJF (Dec–Feb), MAM (Mar–May), JJA (Jun–Aug), SON (Sep–Nov).

    Parameters
    ----------
    df:
        Input DataFrame with a datetime/date *time_col*.
    time_col:
        Name of the time column.

    Returns
    -------
    pl.DataFrame
        Original DataFrame with an added ``season`` column (Utf8).

    Raises
    ------
    ValueError
        If *time_col* is not present in the DataFrame.
    """
    if time_col not in df.columns:
        raise ValueError(f"Column '{time_col}' not found in DataFrame")

    # Build the mapping expression: month → season
    expr = pl.col(time_col).cast(pl.Datetime("us")).dt.month()

    # Chain when/then for each mapping entry
    season_expr = pl.when(expr == 12).then(pl.lit("DJF"))
    for month, season in SEASON_MAP.items():
        if month != 12:  # already handled
            season_expr = season_expr.when(expr == month).then(pl.lit(season))
    season_expr = season_expr.otherwise(pl.lit(None)).alias("season")

    return df.with_columns(season_expr)


def fit_weibull_by_station_and_season(
    df: pl.DataFrame,
    ws_col: str = "ws10",
    station_col: str = "station",
    time_col: str = "time",
) -> pl.DataFrame:
    """Fit Weibull distributions per station **and** per season.

    Parameters
    ----------
    df:
        Input DataFrame with *ws_col*, *station_col*, and *time_col*.
    ws_col:
        Name of the wind-speed column.
    station_col:
        Name of the station identifier column.
    time_col:
        Name of the time column (must be castable to Datetime).

    Returns
    -------
    pl.DataFrame
        Columns: *station_col*, ``season``, ``weibull_k``,
        ``weibull_A``, ``mean_ws``, ``std_ws``, ``n_obs``.

    Raises
    ------
    ValueError
        If required columns are missing.
    """
    for col in (ws_col, station_col, time_col):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")

    # Ensure season column is present
    df_with_season = add_season_column(df, time_col)

    records: list[dict] = []

    stations = sorted(df_with_season[station_col].unique().to_list())

    for station_name in stations:
        for season in SEASONS_ORDER:
            station_season_speeds = (
                df_with_season.filter(
                    (pl.col(station_col) == station_name)
                    & (pl.col("season") == season)
                    & pl.col(ws_col).is_not_null()
                    & (pl.col(ws_col) > 0)
                )[ws_col]
                .to_numpy()
                .astype(np.float64)
            )

            result = fit_weibull(station_season_speeds)

            records.append(
                {
                    station_col: station_name,
                    "season": season,
                    "weibull_k": result[0] if result else None,
                    "weibull_A": result[1] if result else None,
                    "mean_ws": (
                        float(np.mean(station_season_speeds))
                        if len(station_season_speeds) > 0
                        else None
                    ),
                    "std_ws": (
                        float(np.std(station_season_speeds))
                        if len(station_season_speeds) > 0
                        else None
                    ),
                    "n_obs": len(station_season_speeds),
                }
            )

    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# 4. Seasonal variability analysis
# ---------------------------------------------------------------------------


def compute_seasonal_variability(
    seasonal_fits: pl.DataFrame,
    station_col: str = "station",
) -> pl.DataFrame:
    """Compute seasonal variability metrics from per-station-season fits.

    For each station the function computes:

    * **cv_k** – Coefficient of Variation of the Weibull shape parameter
      across seasons: ``std(k) / mean(k)``.
    * **cv_A** – Coefficient of Variation of the Weibull scale parameter.
    * **range_k** – Range of *k* values (``max - min``).
    * **range_A** – Range of *A* values.
    * **best_season** – Season with the highest scale *A* (strongest wind).
    * **worst_season** – Season with the lowest scale *A*.
    * **n_seasons_fitted** – Number of seasons with a successful fit.

    Parameters
    ----------
    seasonal_fits:
        Output of :func:`fit_weibull_by_station_and_season`.
    station_col:
        Name of the station identifier column.

    Returns
    -------
    pl.DataFrame
        One row per station with the variability metrics listed above.

    Raises
    ------
    ValueError
        If required columns are missing in *seasonal_fits*.
    """
    required = {station_col, "season", "weibull_k", "weibull_A"}
    missing = required - set(seasonal_fits.columns)
    if missing:
        raise ValueError(f"Missing columns in seasonal_fits: {missing}")

    # Filter to rows that have successful fits
    valid = seasonal_fits.filter(
        pl.col("weibull_k").is_not_null() & pl.col("weibull_A").is_not_null()
    )

    if valid.height == 0:
        return pl.DataFrame(
            {
                station_col: [],
                "cv_k": [],
                "cv_A": [],
                "range_k": [],
                "range_A": [],
                "best_season": [],
                "worst_season": [],
                "n_seasons_fitted": [],
            }
        )

    records: list[dict] = []

    for station_name in sorted(valid[station_col].unique().to_list()):
        st_data = valid.filter(pl.col(station_col) == station_name)

        k_vals = st_data["weibull_k"].to_numpy()
        a_vals = st_data["weibull_A"].to_numpy()
        seasons = st_data["season"].to_list()
        n_fitted = len(k_vals)

        mean_k = float(np.mean(k_vals))
        std_k = float(np.std(k_vals))
        mean_a = float(np.mean(a_vals))
        std_a = float(np.std(a_vals))

        # Best/worst season based on scale A (proxy for mean wind speed)
        best_idx = int(np.argmax(a_vals))
        worst_idx = int(np.argmin(a_vals))

        records.append(
            {
                station_col: station_name,
                "cv_k": std_k / mean_k if mean_k > 0 else None,
                "cv_A": std_a / mean_a if mean_a > 0 else None,
                "range_k": float(np.max(k_vals) - np.min(k_vals)),
                "range_A": float(np.max(a_vals) - np.min(a_vals)),
                "best_season": seasons[best_idx],
                "worst_season": seasons[worst_idx],
                "n_seasons_fitted": n_fitted,
            }
        )

    return pl.DataFrame(records)


# ---------------------------------------------------------------------------
# 5. Convenience: full Weibull analysis pipeline
# ---------------------------------------------------------------------------


def run_weibull_analysis(
    df: pl.DataFrame,
    ws_col: str = "ws10",
    station_col: str = "station",
    time_col: str = "time",
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Run the complete Weibull analysis pipeline.

    1. Fit Weibull per station (annual).
    2. Fit Weibull per station and season.
    3. Compute seasonal variability metrics.

    Parameters
    ----------
    df:
        Input DataFrame (typically QC-corrected output).
    ws_col, station_col, time_col:
        Column names.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
        ``(annual_fits, seasonal_fits, variability)``
    """
    annual_fits = fit_weibull_by_station(df, ws_col, station_col)
    seasonal_fits = fit_weibull_by_station_and_season(df, ws_col, station_col, time_col)
    variability = compute_seasonal_variability(seasonal_fits, station_col)

    return annual_fits, seasonal_fits, variability
