"""Pipeline step: wind quality control.

Reads the staging Parquet (output of ingest), applies calm corrections
and sector-consistency checks, and writes a cleaned Parquet plus a
station-level QC flags summary.
"""

import os

import polars as pl

from weather.data.quality import run_quality_control


def run_qc(
    input_path: str = "data/staging/all_stations.parquet",
    output_data_path: str = "data/clean/all_stations_qc.parquet",
    output_flags_path: str = "data/clean/station_qc_flags.parquet",
    calm_threshold: float = 0.5,
    n_sectors: int = 12,
    max_deviation: float = 3.0,
) -> None:
    """Execute the full wind QC pipeline on staged data.

    Parameters
    ----------
    input_path:
        Path to the raw Parquet (output of ``ingest.run_ingestion``).
    output_data_path:
        Path to write the corrected DataFrame.
    output_flags_path:
        Path to write the per-station QC flags.
    calm_threshold:
        Wind-speed calm threshold (m/s).
    n_sectors:
        Number of wind-rose sectors for consistency check.
    max_deviation:
        Max allowed sector deviation ratio.
    """
    print(f"Leyendo datos desde {input_path}...")
    df = pl.read_parquet(input_path)
    print(f"  -> {df.height} filas, {df.width} columnas")

    # Determine which height levels are available
    pairs = []
    if "ws10" in df.columns and "wd10" in df.columns:
        pairs.append(("ws10", "wd10"))
    if "ws100" in df.columns and "wd100" in df.columns:
        pairs.append(("ws100", "wd100"))

    all_flags: list[pl.DataFrame] = []

    for ws_col, wd_col in pairs:
        print(f"\nControl de calidad para {ws_col}/{wd_col}...")
        df, flags = run_quality_control(
            df,
            ws_col=ws_col,
            wd_col=wd_col,
            calm_threshold=calm_threshold,
            n_sectors=n_sectors,
            max_deviation=max_deviation,
        )
        # Rename is_calm to include height level suffix
        height = ws_col.replace("ws", "")
        df = df.rename({"is_calm": f"is_calm_{height}"})
        flags = flags.rename(
            {
                "chi2": f"chi2_{height}",
                "max_sector_deviation": f"max_sector_deviation_{height}",
                "flagged": f"flagged_{height}",
            }
        )
        all_flags.append(flags)

        n_calms = df.filter(pl.col(f"is_calm_{height}")).height
        n_flagged = flags.filter(pl.col(f"flagged_{height}")).height
        print(f"  Calmas detectadas ({ws_col} < {calm_threshold} m/s): {n_calms}")
        print(f"  Estaciones con inconsistencia sectorial: {n_flagged}")

    # Merge all flag tables
    if all_flags:
        merged_flags = all_flags[0]
        for extra in all_flags[1:]:
            merged_flags = merged_flags.join(
                extra, on="station", how="full", coalesce=True
            )
    else:
        merged_flags = pl.DataFrame({"station": []})

    # Write outputs
    for path in (output_data_path, output_flags_path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    df.write_parquet(output_data_path)
    merged_flags.write_parquet(output_flags_path)

    print(f"\nDatos corregidos guardados en: {output_data_path}")
    print(f"Flags de QC guardados en: {output_flags_path}")


if __name__ == "__main__":
    run_qc()
