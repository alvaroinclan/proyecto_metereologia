"""
Time series visualization for wind data.
"""

import plotly.express as px
import plotly.graph_objects as go
import polars as pl


def plot_top_stations_timeseries(
    df: pl.DataFrame,
    top_stations: list[str],
    ws_col: str = "ws100",
    title: str = "Velocidad Media Mensual (Top 3 Estaciones)",
) -> go.Figure:
    """
    Plots a time series of the monthly mean wind speed for the specified stations.
    """
    # Filter for top stations
    filtered_df = df.filter(pl.col("station").is_in(top_stations))

    # Compute monthly mean for the combined top stations
    monthly_mean = (
        filtered_df.with_columns(pl.col("time").dt.truncate("1mo").alias("month"))
        .group_by("month")
        .agg(pl.col(ws_col).mean().alias("mean_ws"))
        .sort("month")
    )

    pd_df = monthly_mean.to_pandas()

    fig = px.bar(
        pd_df,
        x="month",
        y="mean_ws",
        title=title,
        labels={"month": "TIEMPO (MESES)", "mean_ws": "VELOCIDAD DEL VIENTO (m/s)"},
        template="plotly_dark",
    )

    # Formatear el eje X para que muestre los meses
    fig.update_xaxes(dtick="M1", tickformat="%b-%y")

    return fig


def plot_stations_monthly_lines(
    df: pl.DataFrame,
    top_stations: list[str],
    ws_col: str = "ws100",
    title: str = "Evolución de la Velocidad Media Mensual por Estación (Top 3)",
) -> go.Figure:
    """
    Plots a time series of the monthly mean wind speed for each of the top stations as lines.
    """
    # Filter for top stations
    filtered_df = df.filter(pl.col("station").is_in(top_stations))

    # Compute monthly mean per station
    monthly_mean = (
        filtered_df.with_columns(pl.col("time").dt.truncate("1mo").alias("month"))
        .group_by(["month", "station"])
        .agg(pl.col(ws_col).mean().alias("mean_ws"))
        .sort("month")
    )

    pd_df = monthly_mean.to_pandas()

    fig = px.line(
        pd_df,
        x="month",
        y="mean_ws",
        color="station",
        title=title,
        markers=True,
        labels={
            "month": "TIEMPO (MESES)",
            "mean_ws": "VELOCIDAD DEL VIENTO (m/s)",
            "station": "Estación",
        },
        template="plotly_dark",
    )

    fig.update_xaxes(dtick="M1", tickformat="%b-%y")

    return fig
