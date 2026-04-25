"""
Wind-specific quality control: calm corrections and sector consistency.

This module provides functions for quality-controlling wind speed and
direction data from ERA5 (or any gridded / station source).  Two main
checks are implemented:

1. **Calm corrections** – When wind speed falls below a configurable
   threshold (default 0.5 m/s, per WMO guidelines), the associated
   wind direction is physically meaningless and is set to ``NaN``.
   An optional ``is_calm`` boolean flag column is added for downstream
   filtering.

2. **Sector consistency** – The 360° wind rose is divided into *N*
   equal sectors.  For each station the observed frequency per sector
   is compared with the uniform expectation (1/N).  If any sector's
   relative deviation exceeds a configurable tolerance, the station is
   flagged as potentially problematic (sensor obstruction, sheltering
   effects, etc.).  A chi-squared goodness-of-fit statistic is also
   computed.
"""

from __future__ import annotations

import polars as pl

# ---------------------------------------------------------------------------
# 1. Calm corrections
# ---------------------------------------------------------------------------

def apply_calm_corrections(
    df: pl.DataFrame,
    ws_col: str = "ws10",
    wd_col: str = "wd10",
    calm_threshold: float = 0.5,
) -> pl.DataFrame:
    """Set wind direction to ``null`` when wind speed is below *calm_threshold*.

    A new boolean column ``is_calm`` is added indicating calm periods.

    Parameters
    ----------
    df:
        Input DataFrame with at least *ws_col* and *wd_col*.
    ws_col:
        Name of the wind-speed column (m/s).
    wd_col:
        Name of the wind-direction column (degrees).
    calm_threshold:
        Speed threshold in m/s below which the observation is
        considered calm.  Default 0.5 m/s (WMO recommendation).

    Returns
    -------
    pl.DataFrame
        DataFrame with corrected *wd_col* and added ``is_calm`` column.

    Raises
    ------
    ValueError
        If *ws_col* or *wd_col* are not present in the DataFrame, or if
        *calm_threshold* is negative.
    """
    if calm_threshold < 0:
        raise ValueError(f"calm_threshold must be >= 0, got {calm_threshold}")
    for col in (ws_col, wd_col):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")

    return df.with_columns(
        pl.when(pl.col(ws_col) < calm_threshold)
        .then(None)
        .otherwise(pl.col(wd_col))
        .alias(wd_col),
        (pl.col(ws_col) < calm_threshold).alias("is_calm"),
    )


# ---------------------------------------------------------------------------
# 2. Sector consistency
# ---------------------------------------------------------------------------

def compute_sector_frequencies(
    df: pl.DataFrame,
    wd_col: str = "wd10",
    station_col: str = "station",
    n_sectors: int = 12,
) -> pl.DataFrame:
    """Compute the relative frequency of wind directions per sector per station.

    Calm periods (``null`` direction) are excluded before computing
    frequencies.

    Parameters
    ----------
    df:
        Input DataFrame.
    wd_col:
        Name of the wind-direction column (degrees, 0–360).
    station_col:
        Name of the station identifier column.
    n_sectors:
        Number of equal-width sectors to divide 360° into.

    Returns
    -------
    pl.DataFrame
        Columns: *station_col*, ``sector``, ``count``, ``freq``
        (relative frequency within the station).
    """
    sector_width = 360.0 / n_sectors

    sector_df = (
        df
        .filter(pl.col(wd_col).is_not_null())
        .with_columns(
            # Sector index 0 .. n_sectors-1
            (pl.col(wd_col) / sector_width).floor().cast(pl.Int32).alias("sector")
        )
        # Handle edge case: direction == 360 → sector should be 0
        .with_columns(
            pl.when(pl.col("sector") >= n_sectors)
            .then(0)
            .otherwise(pl.col("sector"))
            .alias("sector")
        )
        .group_by([station_col, "sector"])
        .agg(pl.len().alias("count"))
    )

    # Compute relative frequency within each station
    total_per_station = (
        sector_df
        .group_by(station_col)
        .agg(pl.col("count").sum().alias("total"))
    )

    sector_df = (
        sector_df
        .join(total_per_station, on=station_col)
        .with_columns(
            (pl.col("count") / pl.col("total")).alias("freq")
        )
        .drop("total")
        .sort([station_col, "sector"])
    )

    return sector_df


def flag_sector_inconsistencies(
    sector_freq_df: pl.DataFrame,
    station_col: str = "station",
    n_sectors: int = 12,
    max_deviation: float = 3.0,
) -> pl.DataFrame:
    """Flag stations whose sector distribution deviates too much from uniform.

    For each station the function computes:

    * **sector_deviation** – the ratio of the observed frequency to the
      expected uniform frequency (1/*n_sectors*).  A value of 2.0 means
      that sector is observed twice as often as expected.
    * **chi2** – Pearson's chi-squared statistic for the full sector
      distribution against the uniform hypothesis.
    * **flagged** – ``True`` if *any* sector's deviation exceeds
      *max_deviation*.

    Parameters
    ----------
    sector_freq_df:
        Output of :func:`compute_sector_frequencies`.
    station_col:
        Name of the station identifier column.
    n_sectors:
        Number of sectors (must match the value used to compute
        *sector_freq_df*).
    max_deviation:
        Maximum allowed ratio ``observed_freq / expected_freq`` before
        a station is flagged.

    Returns
    -------
    pl.DataFrame
        One row per station with columns: *station_col*, ``chi2``,
        ``max_sector_deviation``, ``flagged``.
    """
    expected_freq = 1.0 / n_sectors

    with_deviation = sector_freq_df.with_columns(
        (pl.col("freq") / expected_freq).alias("sector_deviation")
    )

    # Chi-squared: sum over sectors of (obs - expected)^2 / expected
    # We work with frequencies (sum to 1), so expected = 1/n_sectors
    station_stats = (
        with_deviation
        .group_by(station_col)
        .agg(
            # chi2 = N * sum_i (f_i - e)^2 / e  — but since we have
            # frequencies we use the simplified form:
            (
                ((pl.col("freq") - expected_freq) ** 2 / expected_freq)
                .sum()
            ).alias("chi2"),
            pl.col("sector_deviation").max().alias("max_sector_deviation"),
            pl.col("count").sum().alias("total_obs"),
        )
        # Scale chi2 by total observations to get Pearson chi-squared
        .with_columns(
            (pl.col("chi2") * pl.col("total_obs")).alias("chi2"),
        )
        .with_columns(
            (pl.col("max_sector_deviation") > max_deviation).alias("flagged"),
        )
        .sort(station_col)
    )

    return station_stats


def run_quality_control(
    df: pl.DataFrame,
    ws_col: str = "ws10",
    wd_col: str = "wd10",
    station_col: str = "station",
    calm_threshold: float = 0.5,
    n_sectors: int = 12,
    max_deviation: float = 3.0,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run the full wind quality-control pipeline.

    1. Apply calm corrections.
    2. Compute sector frequencies and flag inconsistent stations.

    Parameters
    ----------
    df:
        Raw DataFrame with wind speed/direction (output of ingest step).
    ws_col, wd_col, station_col:
        Column names.
    calm_threshold:
        Calm-speed threshold in m/s.
    n_sectors:
        Number of wind-rose sectors.
    max_deviation:
        Max sector deviation ratio before flagging.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame]
        ``(df_corrected, station_flags)`` — the corrected DataFrame and
        a summary table of per-station QC flags.
    """
    df_corrected = apply_calm_corrections(df, ws_col, wd_col, calm_threshold)

    sector_freq = compute_sector_frequencies(
        df_corrected, wd_col, station_col, n_sectors
    )
    station_flags = flag_sector_inconsistencies(
        sector_freq, station_col, n_sectors, max_deviation
    )

    return df_corrected, station_flags
