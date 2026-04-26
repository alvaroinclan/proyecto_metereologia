"""
Annual Energy Production (AEP) calculations.

This module provides functions to calculate both theoretical AEP
(using fitted Weibull distributions) and empirical AEP (using raw
time-series data) by mapping wind speeds to a wind turbine's power curve.
"""

from __future__ import annotations

import numpy as np
import polars as pl

HOURS_PER_YEAR = 8760.0


def get_reference_power_curve() -> pl.DataFrame:
    """
    Generate a reference wind turbine power curve.

    Based loosely on a typical 2.0 MW turbine (e.g., Vestas V90).
    - Cut-in wind speed: 4 m/s
    - Rated wind speed: ~12-13 m/s
    - Cut-out wind speed: 25 m/s
    - Rated power: 2000 kW

    Returns
    -------
    pl.DataFrame
        DataFrame with columns 'wind_speed' (m/s) and 'power_kw' (kW).
    """
    speeds = np.arange(0, 30.1, 0.5)

    # Interpolation points for the power curve
    v_points = np.array([0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 25, 25.1, 30])
    p_points = np.array(
        [
            0,
            0,
            65,
            145,
            258,
            411,
            607,
            849,
            1146,
            1521,
            1813,
            1957,
            1993,
            2000,
            2000,
            0,
            0,
        ]
    )

    power_kw = np.interp(speeds, v_points, p_points)

    return pl.DataFrame({"wind_speed": speeds, "power_kw": power_kw})


def _calculate_aep_vectorized_cdf(
    k_vals: np.ndarray, a_vals: np.ndarray, v_vals: np.ndarray, p_vals: np.ndarray
) -> np.ndarray:
    """
    Core vectorized numpy computation for theoretical AEP using the Weibull CDF.
    """
    K = k_vals[:, None]
    A = a_vals[:, None]
    V = v_vals[None, :]
    P = p_vals[None, :]

    dv = V[0, 1] - V[0, 0] if V.shape[1] > 1 else 1.0

    V_upper = V + dv / 2
    V_lower = np.maximum(0, V - dv / 2)

    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        cdf_upper = 1.0 - np.exp(-((V_upper / A) ** K))
        cdf_lower = 1.0 - np.exp(-((V_lower / A) ** K))
        prob = cdf_upper - cdf_lower

        expected_power = np.nansum(P * prob, axis=1)
        aep = expected_power * HOURS_PER_YEAR / 1e6

    # Mask out results where inputs were NaN
    aep[np.isnan(k_vals) | np.isnan(a_vals)] = np.nan
    return aep


def compute_theoretical_aep(
    weibull_df: pl.DataFrame,
    power_curve_df: pl.DataFrame,
    k_col: str = "weibull_k",
    a_col: str = "weibull_A",
    ws_col: str = "wind_speed",
    power_col: str = "power_kw",
    aep_col_name: str = "theoretical_aep_gwh",
) -> pl.DataFrame:
    """
    Compute theoretical AEP for each station using its Weibull parameters.

    Parameters
    ----------
    weibull_df : pl.DataFrame
        DataFrame containing Weibull shape (k) and scale (A) parameters.
    power_curve_df : pl.DataFrame
        DataFrame representing the turbine power curve.
    k_col : str
        Name of the column containing the Weibull shape parameter 'k'.
    a_col : str
        Name of the column containing the Weibull scale parameter 'A'.
    ws_col : str
        Name of the wind speed column in the power curve DataFrame.
    power_col : str
        Name of the power output column (in kW) in the power curve DataFrame.
    aep_col_name : str
        Name of the resulting AEP column.

    Returns
    -------
    pl.DataFrame
        The original DataFrame with an added column for the theoretical AEP in GWh.
    """
    for col in (k_col, a_col):
        if col not in weibull_df.columns:
            raise ValueError(f"Column '{col}' not found in Weibull DataFrame")

    for col in (ws_col, power_col):
        if col not in power_curve_df.columns:
            raise ValueError(f"Column '{col}' not found in Power Curve DataFrame")

    v_vals = power_curve_df[ws_col].to_numpy()
    p_vals = power_curve_df[power_col].to_numpy()

    k_vals = weibull_df[k_col].to_numpy()
    a_vals = weibull_df[a_col].to_numpy()

    aep_vals = _calculate_aep_vectorized_cdf(k_vals, a_vals, v_vals, p_vals)

    return weibull_df.with_columns(
        pl.Series(aep_col_name, aep_vals).cast(pl.Float64).fill_nan(None)
    )


def compute_empirical_aep(
    time_series_df: pl.DataFrame,
    power_curve_df: pl.DataFrame,
    ws_col: str = "ws10",
    power_col: str = "power_kw",
    station_col: str = "station",
    aep_col_name: str = "empirical_aep_gwh",
) -> pl.DataFrame:
    """
    Compute empirical AEP by mapping hourly wind speed directly to the power curve.

    Parameters
    ----------
    time_series_df : pl.DataFrame
        Raw or QC'd time-series DataFrame with wind speeds.
    power_curve_df : pl.DataFrame
        DataFrame representing the turbine power curve.
    ws_col : str
        Name of the wind speed column in time_series_df.
    power_col : str
        Name of the power output column (in kW) in the power curve DataFrame.
    station_col : str
        Name of the station identifier column.
    aep_col_name : str
        Name of the resulting AEP column.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns for station and empirical AEP in GWh.
    """
    for col in (ws_col, station_col):
        if col not in time_series_df.columns:
            raise ValueError(f"Column '{col}' not found in time-series DataFrame")

    pc_ws_col = [c for c in power_curve_df.columns if c != power_col][0]
    if "wind_speed" in power_curve_df.columns:
        pc_ws_col = "wind_speed"

    pc_sorted = power_curve_df.select([pc_ws_col, power_col]).sort(pc_ws_col)

    ts_sorted = time_series_df.filter(pl.col(ws_col).is_not_null()).sort(ws_col)

    # Asof join strategy "nearest" interpolates to the nearest bin
    joined = ts_sorted.join_asof(
        pc_sorted, left_on=ws_col, right_on=pc_ws_col, strategy="nearest"
    )

    aep_df = (
        joined.group_by(station_col)
        .agg(
            pl.col(power_col).mean().alias("mean_power_kw"),
            pl.len().alias("n_valid_hours"),
        )
        .with_columns(
            (pl.col("mean_power_kw") * HOURS_PER_YEAR / 1e6).alias(aep_col_name)
        )
        .drop("mean_power_kw")
    )

    return aep_df.sort(station_col)


def rank_locations(
    aep_df: pl.DataFrame,
    aep_col: str = "theoretical_aep_gwh",
    rank_col_name: str = "rank",
) -> pl.DataFrame:
    """
    Rank stations based on AEP (highest to lowest).

    Parameters
    ----------
    aep_df : pl.DataFrame
        DataFrame containing an AEP column.
    aep_col : str
        Name of the column containing the AEP values to sort by.
    rank_col_name : str
        Name of the new rank column to create.

    Returns
    -------
    pl.DataFrame
        DataFrame sorted by AEP descending, with a new rank column.
    """
    if aep_col not in aep_df.columns:
        raise ValueError(f"Column '{aep_col}' not found in DataFrame")

    return aep_df.sort(aep_col, descending=True, nulls_last=True).with_columns(
        pl.int_range(1, pl.len() + 1).alias(rank_col_name)
    )
