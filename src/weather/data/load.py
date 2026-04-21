from pathlib import Path

import polars as pl


def load_station_csv(path: str) -> pl.DataFrame:
    """
    Load a single station CSV and add station_id column.
    """
    path = Path(path)
    station_id = path.stem

    df = pl.read_csv(path, skip_rows=13)

    df = df.with_columns(
        pl.lit(station_id).alias("station")
    )

    return df


def load_all_stations(folder: str) -> pl.DataFrame:
    """
    Load all CSV files from a folder into a single DataFrame.
    """
    folder = Path(folder)
    files = folder.glob("*.csv")

    dfs = [load_station_csv(f) for f in files]

    return pl.concat(dfs, how="diagonal")

