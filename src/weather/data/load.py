from pathlib import Path

import cfgrib
import numpy as np
import polars as pl
import xarray as xr


def generate_target_locations() -> tuple[xr.DataArray, xr.DataArray, list[str]]:
    """
    Generate 50 target locations in the north of Spain using ECMWF 0.1 degree grid nodes.
    Returns latitude DataArray, longitude DataArray, and list of station IDs.
    """
    # Define 5 latitudes and 10 longitudes to get 50 points in Northern Spain
    # roughly covering Asturias/Cantabria/Basque Country
    lats = np.arange(42.8, 43.3, 0.1)  # 5 points: 42.8, 42.9, 43.0, 43.1, 43.2
    lons = np.arange(-6.0, -5.0, 0.1)  # 10 points: -6.0 to -5.1

    # Create meshgrid
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    lon_flat = lon_grid.flatten()
    lat_flat = lat_grid.flatten()

    station_ids = [f"station_{i}" for i in range(len(lon_flat))]

    # Create DataArrays for advanced interpolation
    lat_da = xr.DataArray(lat_flat, dims="station", coords={"station": station_ids})
    lon_da = xr.DataArray(lon_flat, dims="station", coords={"station": station_ids})

    return lat_da, lon_da, station_ids


def process_dataset_chunk(
    ds: xr.Dataset, lat_da: xr.DataArray, lon_da: xr.DataArray
) -> pl.DataFrame:
    """
    Interpolate dataset to target locations and convert to Polars DataFrame.
    """
    # Interpolate using linear method to get exactly the 0.1 degree nodes
    # Note: original data is 0.25 degree
    ds_interp = ds.interp(latitude=lat_da, longitude=lon_da, method="linear")

    # Convert to pandas first (xarray to polars direct is not available)
    df = ds_interp.to_dataframe().reset_index()

    # If the dataset has 'valid_time' instead of 'time', rename it
    if "valid_time" in df.columns and "time" in ds.dims and "step" in ds.dims:
        df = df.drop(columns=["time", "step"])
        df = df.rename(columns={"valid_time": "time"})

    # Drop irrelevant columns
    cols_to_drop = ["number", "surface", "latitude", "longitude"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    return pl.from_pandas(df)


def load_grib_data_in_batches(grib_path: str, batch_size: int = 10) -> pl.DataFrame:
    """
    Load GRIB file, interpolate to 50 locations, and process in batches of locations.
    """
    path = Path(grib_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    print(f"Opening GRIB datasets from {path}...")
    datasets = cfgrib.open_datasets(str(path))

    # Identify which dataset is which based on variables
    ds_wind = None
    ds_gust = None
    for ds in datasets:
        if "u10" in ds.variables:
            ds_wind = ds
        elif "fg10" in ds.variables:
            ds_gust = ds

    lat_da, lon_da, station_ids = generate_target_locations()

    all_dfs = []

    # Batch processing by location
    total_locations = len(station_ids)
    print(f"Processing {total_locations} locations in batches of {batch_size}...")

    for i in range(0, total_locations, batch_size):
        batch_lats = lat_da.isel(station=slice(i, i + batch_size))
        batch_lons = lon_da.isel(station=slice(i, i + batch_size))

        print(
            f"  Processing batch {i // batch_size + 1}/{(total_locations - 1) // batch_size + 1}..."
        )

        df_wind = None
        if ds_wind is not None:
            df_wind = process_dataset_chunk(ds_wind, batch_lats, batch_lons)

        df_gust = None
        if ds_gust is not None:
            df_gust = process_dataset_chunk(ds_gust, batch_lats, batch_lons)

        # Merge if both exist
        if df_wind is not None and df_gust is not None:
            # We must ensure 'time' and 'station' exist and align
            # The gust dataset might have NaNs for some hours since it's every 12h forecast
            df_batch = df_wind.join(df_gust, on=["time", "station"], how="left")
        else:
            df_batch = df_wind if df_wind is not None else df_gust

        all_dfs.append(df_batch)

    # Combine all batches
    final_df = pl.concat(all_dfs)

    # Calculate wind speed and direction (vectorial calculation)
    if "u10" in final_df.columns and "v10" in final_df.columns:
        final_df = final_df.with_columns(
            ws10=np.sqrt(pl.col("u10") ** 2 + pl.col("v10") ** 2),
            wd10=(180 / np.pi * np.arctan2(pl.col("u10"), pl.col("v10")) + 180) % 360,
        )

    if "u100" in final_df.columns and "v100" in final_df.columns:
        final_df = final_df.with_columns(
            ws100=np.sqrt(pl.col("u100") ** 2 + pl.col("v100") ** 2),
            wd100=(180 / np.pi * np.arctan2(pl.col("u100"), pl.col("v100")) + 180)
            % 360,
        )

    return final_df
