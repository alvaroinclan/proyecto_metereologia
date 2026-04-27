from datetime import datetime

import folium
import plotly.graph_objects as go
import polars as pl
import pytest

from weather.visualization.maps import attach_coordinates, create_potential_map
from weather.visualization.timeseries import (
    plot_stations_monthly_lines,
    plot_top_stations_timeseries,
)
from weather.visualization.wind_rose import compute_wind_rose_data, plot_wind_rose


def test_compute_wind_rose_data():
    df = pl.DataFrame(
        {
            "ws10": [0.0, 5.0, 9.0, 15.0, 25.0, None],
            "wd10": [0.0, 90.0, 180.0, 270.0, 359.0, 0.0],
        }
    )

    rose = compute_wind_rose_data(df)

    assert rose.height > 0
    assert "direction" in rose.columns
    assert "speed_bin" in rose.columns
    assert "frequency_pct" in rose.columns

    # Check that percentage sums to roughly 100%
    assert 99.0 <= rose["frequency_pct"].sum() <= 101.0


def test_compute_wind_rose_data_empty():
    df = pl.DataFrame({"ws10": [], "wd10": []})
    rose = compute_wind_rose_data(df)
    assert rose.height == 0


def test_compute_wind_rose_data_value_error():
    df = pl.DataFrame({"ws10": [5.0], "wd10": [90.0]})
    with pytest.raises(ValueError, match="Length of speed_labels"):
        compute_wind_rose_data(df, speed_bins=(0, 4, 8), speed_labels=("0-4",))


def test_compute_wind_rose_data_custom_sectors_and_station():
    df = pl.DataFrame({
        "station": ["st1", "st1", "st2"],
        "ws10": [5.0, 10.0, 5.0],
        "wd10": [0.0, 45.0, 90.0]
    })
    rose = compute_wind_rose_data(df, n_sectors=8, station_col="station")
    assert rose.height > 0
    assert "station" in rose.columns
    assert "direction" in rose.columns
    # Check if direction has custom format like "0.0°"
    dirs = rose["direction"].to_list()
    assert any("°" in d for d in dirs)


def test_plot_wind_rose():
    df = pl.DataFrame(
        {
            "direction": ["N", "E", "S", "W"],
            "speed_bin": ["0-4", "4-8", "8-12", "12-16"],
            "frequency_pct": [25.0, 25.0, 25.0, 25.0],
        }
    )
    fig = plot_wind_rose(df)
    assert isinstance(fig, go.Figure)


def test_attach_coordinates():
    df = pl.DataFrame({"station": ["station_0", "station_49", "unknown_station"]})
    res = attach_coordinates(df)

    assert "latitude" in res.columns
    assert "longitude" in res.columns
    assert res.filter(pl.col("station") == "station_0")["latitude"].is_not_null()[0]
    assert res.filter(pl.col("station") == "unknown_station")["latitude"].is_null()[0]


def test_create_potential_map():
    df = pl.DataFrame(
        {
            "station": ["station_0", "station_1"],
            "theoretical_aep_gwh": [1.0, 2.0],
            "latitude": [43.0, 43.1],
            "longitude": [-5.5, -5.6],
        }
    )

    m = create_potential_map(df)
    assert isinstance(m, folium.Map)

    # Test without coordinates automatically attaching them
    df_no_coords = pl.DataFrame(
        {
            "station": ["station_0", "station_1"],
            "theoretical_aep_gwh": [1.0, 2.0],
        }
    )
    m2 = create_potential_map(df_no_coords)
    assert isinstance(m2, folium.Map)


def test_create_potential_map_empty():
    df = pl.DataFrame({"station": [], "theoretical_aep_gwh": [], "latitude": [], "longitude": []})
    m = create_potential_map(df)
    assert isinstance(m, folium.Map)


def test_create_potential_map_with_all_stats():
    df = pl.DataFrame({
        "station": ["station_0"],
        "theoretical_aep_gwh": [1.0],
        "latitude": [43.0],
        "longitude": [-5.5],
        "empirical_aep_gwh": [0.9],
        "weibull_A": [6.5],
        "weibull_k": [2.1],
        "rank": [1]
    })
    m = create_potential_map(df)
    assert isinstance(m, folium.Map)


def test_plot_top_stations_timeseries():
    df = pl.DataFrame(
        {
            "time": [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 2, 1)],
            "station": ["station_1", "station_1", "station_2"],
            "ws100": [10.0, 12.0, 15.0],
        }
    )
    fig = plot_top_stations_timeseries(df, top_stations=["station_1", "station_2"])
    assert isinstance(fig, go.Figure)


def test_plot_stations_monthly_lines():
    df = pl.DataFrame(
        {
            "time": [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 2, 1)],
            "station": ["station_1", "station_1", "station_2"],
            "ws100": [10.0, 12.0, 15.0],
        }
    )
    fig = plot_stations_monthly_lines(df, top_stations=["station_1", "station_2"])
    assert isinstance(fig, go.Figure)
