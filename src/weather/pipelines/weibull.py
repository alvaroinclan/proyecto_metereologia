"""Pipeline step: Weibull distribution fitting and seasonal analysis.

Reads the QC-corrected Parquet, fits two-parameter Weibull distributions
per station (annual and seasonal), computes seasonal variability metrics,
and writes the results as Parquet files.
"""

import os

import polars as pl

from weather.data.weibull import run_weibull_analysis


def run_weibull_pipeline(
    input_path: str = "data/clean/all_stations_qc.parquet",
    output_annual_path: str = "data/results/weibull_annual.parquet",
    output_seasonal_path: str = "data/results/weibull_seasonal.parquet",
    output_variability_path: str = "data/results/weibull_variability.parquet",
) -> None:
    """Execute the full Weibull analysis pipeline.

    Parameters
    ----------
    input_path:
        Path to the QC-corrected Parquet (output of ``qc.run_qc``).
    output_annual_path:
        Path to write the annual (per-station) Weibull fits.
    output_seasonal_path:
        Path to write the per-station-per-season Weibull fits.
    output_variability_path:
        Path to write the seasonal variability summary.
    """
    print(f"Leyendo datos QC desde {input_path}...")
    df = pl.read_parquet(input_path)
    print(f"  -> {df.height} filas, {df.width} columnas")

    # Determine which wind-speed columns are available
    ws_cols = [c for c in ("ws10", "ws100") if c in df.columns]

    if not ws_cols:
        raise RuntimeError(
            "No se encontraron columnas de velocidad de viento (ws10, ws100)"
        )

    for ws_col in ws_cols:
        height = ws_col.replace("ws", "")
        print(f"\n=== Ajuste Weibull para altura {height} m ({ws_col}) ===")

        annual, seasonal, variability = run_weibull_analysis(
            df, ws_col=ws_col, station_col="station", time_col="time"
        )

        # Add height suffix to output paths
        annual_path = output_annual_path.replace(".parquet", f"_{height}m.parquet")
        seasonal_path = output_seasonal_path.replace(".parquet", f"_{height}m.parquet")
        variab_path = output_variability_path.replace(".parquet", f"_{height}m.parquet")

        for path in (annual_path, seasonal_path, variab_path):
            os.makedirs(os.path.dirname(path), exist_ok=True)

        annual.write_parquet(annual_path)
        seasonal.write_parquet(seasonal_path)
        variability.write_parquet(variab_path)

        # Print summary statistics
        fitted_annual = annual.filter(pl.col("weibull_k").is_not_null())
        print(
            f"  Estaciones con ajuste anual exitoso: {fitted_annual.height}/{annual.height}"
        )

        if fitted_annual.height > 0:
            mean_k = fitted_annual["weibull_k"].mean()
            mean_a = fitted_annual["weibull_A"].mean()
            print(f"  k medio (forma): {mean_k:.3f}")
            print(f"  A medio (escala): {mean_a:.3f} m/s")

        fitted_seasonal = seasonal.filter(pl.col("weibull_k").is_not_null())
        print(
            f"  Ajustes estacionales exitosos: {fitted_seasonal.height}/{seasonal.height}"
        )

        if variability.height > 0:
            mean_cv_a = variability["cv_A"].mean()
            print(f"  CV medio de A entre estaciones: {mean_cv_a:.3f}")

            best_counts = (
                variability.group_by("best_season").len().sort("len", descending=True)
            )
            print("  Mejor estación por frecuencia:")
            for row in best_counts.iter_rows(named=True):
                print(f"    {row['best_season']}: {row['len']} estaciones")

        print(f"  Ajustes anuales guardados en:    {annual_path}")
        print(f"  Ajustes estacionales guardados en: {seasonal_path}")
        print(f"  Variabilidad guardada en:        {variab_path}")


if __name__ == "__main__":
    run_weibull_pipeline()
