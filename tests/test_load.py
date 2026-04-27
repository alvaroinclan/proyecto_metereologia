import os

import numpy as np
import polars as pl
import pytest
import xarray as xr

from unittest.mock import patch

from weather.data.load import (
    generate_target_locations,
    load_grib_data_in_batches,
    process_dataset_chunk,
)


def test_generate_target_locations():
    lat_da, lon_da, station_ids = generate_target_locations()

    # Basic shape and size checks
    assert len(station_ids) == 50
    assert lat_da.shape == (50,)
    assert lon_da.shape == (50,)

    # Uniqueness check
    assert len(set(station_ids)) == 50
    assert station_ids[0] == "station_0"

    # Check geographic boundaries (North of Spain bounds)
    assert float(lat_da.min()) >= 42.0
    assert float(lat_da.max()) <= 44.0
    assert float(lon_da.min()) >= -10.0
    assert float(lon_da.max()) <= 0.0


def test_process_dataset_chunk():
    """Test the dataset interpolation and polars conversion independently."""
    # Create a mock xarray dataset
    times = [np.datetime64("2025-01-01T00:00:00")]
    lats = [43.0, 43.25]
    lons = [-6.0, -5.75]

    data = np.random.rand(1, 2, 2)
    ds_mock = xr.Dataset(
        data_vars=dict(
            u10=(["time", "latitude", "longitude"], data),
            v10=(["time", "latitude", "longitude"], data),
        ),
        coords=dict(
            time=times,
            latitude=lats,
            longitude=lons,
            number=0,  # this should be dropped
        ),
    )

    # Generate mock target locations (just 2 points for simplicity)
    target_lats = xr.DataArray(
        [43.1, 43.15], dims="station", coords={"station": ["st_1", "st_2"]}
    )
    target_lons = xr.DataArray(
        [-5.9, -5.8], dims="station", coords={"station": ["st_1", "st_2"]}
    )

    # Process
    df = process_dataset_chunk(ds_mock, target_lats, target_lons)

    # Verify outputs
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 4)  # time, station, u10, v10

    cols = df.columns
    assert "time" in cols
    assert "station" in cols
    assert "u10" in cols
    assert "v10" in cols

    # Verify dropped columns
    assert "latitude" not in cols
    assert "longitude" not in cols
    assert "number" not in cols
    assert "surface" not in cols


def test_process_dataset_chunk_valid_time():
    """Test the dataset interpolation with valid_time."""
    times = [np.datetime64("2025-01-01T00:00:00")]
    lats = [43.0, 43.25]
    lons = [-6.0, -5.75]

    data = np.random.rand(1, 2, 2)
    ds_mock = xr.Dataset(
        data_vars=dict(
            u10=(["time", "latitude", "longitude"], data),
        ),
        coords=dict(
            time=times,
            step=times,
            valid_time=(["time"], times),
            latitude=lats,
            longitude=lons,
        ),
    )

    target_lats = xr.DataArray(
        [43.1, 43.15], dims="station", coords={"station": ["st_1", "st_2"]}
    )
    target_lons = xr.DataArray(
        [-5.9, -5.8], dims="station", coords={"station": ["st_1", "st_2"]}
    )

    df = process_dataset_chunk(ds_mock, target_lats, target_lons)

    assert isinstance(df, pl.DataFrame)
    cols = df.columns
    assert "time" in cols
    assert "step" not in cols
    assert "valid_time" not in cols


@pytest.mark.skipif(
    not os.path.exists("data/raw/data.grib"), reason="GRIB file not found"
)
def test_load_grib_data_in_batches():
    df = load_grib_data_in_batches("data/raw/data.grib", batch_size=50)

    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] > 0
    assert "station" in df.columns
    assert "time" in df.columns

    # Check for wind speed and direction columns
    if "u10" in df.columns:
        assert "ws10" in df.columns
        assert "wd10" in df.columns

        # Velocity should be non-negative
        assert df["ws10"].min() >= 0.0

        # Direction should be between 0 and 360 degrees
        assert df["wd10"].min() >= 0.0
        assert df["wd10"].max() <= 360.0


def test_load_grib_data_in_batches_mocked():
    times = [np.datetime64("2025-01-01T00:00:00")]
    lats = [43.0, 43.1]
    lons = [-6.0, -5.9]
    data = np.array([[[1.0, 2.0], [3.0, 4.0]]])
    
    ds_wind = xr.Dataset(
        data_vars=dict(
            u10=(["time", "latitude", "longitude"], data),
            v10=(["time", "latitude", "longitude"], data),
            u100=(["time", "latitude", "longitude"], data),
            v100=(["time", "latitude", "longitude"], data)
        ),
        coords=dict(time=times, latitude=lats, longitude=lons)
    )

    ds_gust = xr.Dataset(
        data_vars=dict(
            fg10=(["time", "latitude", "longitude"], data)
        ),
        coords=dict(time=times, latitude=lats, longitude=lons)
    )
    
    with patch("weather.data.load.cfgrib.open_datasets", return_value=[ds_wind, ds_gust]):
        with patch("weather.data.load.Path.exists", return_value=True):
            df = load_grib_data_in_batches("dummy.grib", batch_size=10)
            
            assert isinstance(df, pl.DataFrame)
            assert "ws10" in df.columns
            assert "wd10" in df.columns
            assert "ws100" in df.columns
            assert "wd100" in df.columns


def test_load_grib_data_in_batches_file_not_found():
    with pytest.raises(FileNotFoundError, match="File not found"):
        load_grib_data_in_batches("nonexistent_file.grib")


@pytest.mark.skipif(
    not os.path.exists("data/staging/all_stations.parquet"),
    reason="Parquet file not generated yet",
)
def test_generated_parquet():
    """Verify that the generated Parquet file in the staging layer is correct."""
    parquet_path = "data/staging/all_stations.parquet"

    # Check if the file exists and has a reasonable size
    assert os.path.exists(parquet_path)
    assert os.path.getsize(parquet_path) > 0

    # Read the parquet file
    df = pl.read_parquet(parquet_path)

    # Basic shape checks
    assert df.shape[0] > 0

    # Check that required columns are present
    expected_cols = ["station", "time"]
    for col in expected_cols:
        assert col in df.columns

    # Check for calculated vector variables
    # At least ws10 and wd10 should be present if u10 and v10 were present
    assert "ws10" in df.columns
    assert "wd10" in df.columns

    # Ensure there are 50 unique stations
    unique_stations = df["station"].unique().to_list()
    assert len(unique_stations) == 50
    assert "station_0" in unique_stations

    # Verify vector calculations logic on the saved dataframe
    assert df["ws10"].min() >= 0.0
    assert df["wd10"].min() >= 0.0
    assert df["wd10"].max() <= 360.0

    # Check that there are no completely null rows for the wind speed
    # Note: Some missing data might be possible depending on GRIB coverage,
    # but the entire column shouldn't be null.
    assert df["ws10"].is_null().sum() < df.shape[0]
