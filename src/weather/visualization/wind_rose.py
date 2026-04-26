"""
Interactive Wind Rose generation using Plotly.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl


def compute_wind_rose_data(
    df: pl.DataFrame,
    ws_col: str = "ws10",
    wd_col: str = "wd10",
    n_sectors: int = 16,
    speed_bins: tuple[float, ...] = (0, 4, 8, 12, 16, 20, 100),
    speed_labels: tuple[str, ...] = ("0-4", "4-8", "8-12", "12-16", "16-20", ">20"),
    station_col: str | None = None,
) -> pl.DataFrame:
    """
    Computes aggregated frequency data for a wind rose.
    """
    if len(speed_bins) - 1 != len(speed_labels):
        raise ValueError("Length of speed_labels must be length of speed_bins - 1")

    valid = df.filter(pl.col(ws_col).is_not_null() & pl.col(wd_col).is_not_null())

    if valid.height == 0:
        return pl.DataFrame()

    sector_width = 360.0 / n_sectors

    # Compute sector index
    # To center North at 0°, shift by sector_width / 2
    shifted_dir = (valid[wd_col] + sector_width / 2) % 360

    # Bin speeds using a succession of when/then
    expr = pl.when(False).then(pl.lit(None))
    for i in range(len(speed_labels)):
        lower = speed_bins[i]
        upper = speed_bins[i + 1]
        expr = expr.when((pl.col(ws_col) >= lower) & (pl.col(ws_col) < upper)).then(
            pl.lit(speed_labels[i])
        )

    valid = valid.with_columns(
        (shifted_dir / sector_width).floor().cast(pl.Int32).alias("sector_idx"),
        expr.alias("speed_bin"),
    ).filter(pl.col("speed_bin").is_not_null())

    # Map sector index to direction string (N, NNE, NE, etc.)
    dirs = [
        "N",
        "NNE",
        "NE",
        "ENE",
        "E",
        "ESE",
        "SE",
        "SSE",
        "S",
        "SSW",
        "SW",
        "WSW",
        "W",
        "WNW",
        "NW",
        "NNW",
    ]
    if n_sectors != 16:
        dirs = [f"{i * sector_width:.1f}°" for i in range(n_sectors)]

    valid = valid.with_columns(
        pl.col("sector_idx")
        .map_elements(lambda idx: dirs[idx], return_dtype=pl.Utf8)
        .alias("direction")
    )

    group_cols = ["direction", "speed_bin"]
    if station_col:
        group_cols.insert(0, station_col)

    grouped = valid.group_by(group_cols).agg(pl.len().alias("count"))

    # Compute percentage
    total_col = "total"
    if station_col:
        total = grouped.group_by(station_col).agg(
            pl.col("count").sum().alias(total_col)
        )
        grouped = grouped.join(total, on=station_col)
    else:
        grouped = grouped.with_columns(pl.col("count").sum().alias(total_col))

    return grouped.with_columns(
        (pl.col("count") / pl.col(total_col) * 100).alias("frequency_pct")
    ).drop(total_col)


def plot_wind_rose(rose_data: pl.DataFrame, title: str = "Wind Rose") -> go.Figure:
    """
    Creates an interactive Plotly wind rose figure.
    """
    # Ensure standard order for bins and directions
    dirs = [
        "N",
        "NNE",
        "NE",
        "ENE",
        "E",
        "ESE",
        "SE",
        "SSE",
        "S",
        "SSW",
        "SW",
        "WSW",
        "W",
        "WNW",
        "NW",
        "NNW",
    ]
    speed_labels = ("0-4", "4-8", "8-12", "12-16", "16-20", ">20")

    pd_df = rose_data.to_pandas()

    # Sort dataframe to keep colors consistent
    pd_df["dir_cat"] = pd.Categorical(pd_df["direction"], categories=dirs, ordered=True)
    pd_df["speed_cat"] = pd.Categorical(
        pd_df["speed_bin"], categories=speed_labels, ordered=True
    )
    pd_df = pd_df.sort_values(["dir_cat", "speed_cat"])

    fig = px.bar_polar(
        pd_df,
        r="frequency_pct",
        theta="direction",
        color="speed_bin",
        color_discrete_sequence=px.colors.sequential.Plasma_r,
        title=title,
        template="plotly_dark",
        labels={
            "frequency_pct": "Frecuencia (%)",
            "speed_bin": "Velocidad (m/s)",
            "direction": "Dirección",
        },
    )

    fig.update_layout(polar=dict(angularaxis=dict(direction="clockwise", rotation=90)))
    return fig
