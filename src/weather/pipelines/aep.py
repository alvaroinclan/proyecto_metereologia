"""Pipeline step: AEP calculation.

Calculates Theoretical AEP from Weibull parameters and Empirical AEP
from raw time series data using a standard 2.0 MW wind turbine power curve,
and creates a ranking of the most productive locations.
"""

import os

import polars as pl

from weather.data.aep import (
    compute_empirical_aep,
    compute_theoretical_aep,
    get_reference_power_curve,
    rank_locations,
)


def run_aep_pipeline(
    ts_input_path: str = "data/clean/all_stations_qc.parquet",
    weibull_annual_dir: str = "data/results",
    output_aep_path: str = "data/results/aep_ranking.parquet",
) -> None:
    """Execute the AEP calculation and ranking pipeline.

    Parameters
    ----------
    ts_input_path:
        Path to the QC-corrected Parquet (output of ``qc.run_qc``).
    weibull_annual_dir:
        Directory containing the annual Weibull fits.
    output_aep_path:
        Path to write the final AEP results and rankings.
    """
    print("Cargando curva de potencia de referencia (2.0 MW)...")
    pc_df = get_reference_power_curve()

    print(f"Leyendo series temporales QC desde {ts_input_path}...")
    ts_df = pl.read_parquet(ts_input_path)

    # Identificar las alturas disponibles en las series temporales
    ws_cols = [c for c in ("ws10", "ws100") if c in ts_df.columns]

    if not ws_cols:
        raise RuntimeError(
            "No se encontraron columnas de viento en las series temporales"
        )

    for ws_col in ws_cols:
        height = ws_col.replace("ws", "")
        weibull_file = os.path.join(
            weibull_annual_dir, f"weibull_annual_{height}m.parquet"
        )

        if not os.path.exists(weibull_file):
            print(
                f"ADVERTENCIA: No se encontró {weibull_file}. Saltando altura {height}m."
            )
            continue

        print(f"\n=== Cálculo de AEP para altura {height} m ({ws_col}) ===")
        weibull_df = pl.read_parquet(weibull_file)

        # 1. Calcular AEP teórico a partir de los ajustes de Weibull
        weibull_aep = compute_theoretical_aep(weibull_df, pc_df)

        # 2. Calcular AEP empírico a partir de las series temporales
        emp_aep = compute_empirical_aep(ts_df, pc_df, ws_col=ws_col)

        # 3. Unir resultados y generar ranking
        final_aep = weibull_aep.join(emp_aep, on="station", how="left")

        # Clasificar según el AEP teórico
        final_aep = rank_locations(final_aep, aep_col="theoretical_aep_gwh")

        # Guardar resultados
        out_path = output_aep_path.replace(".parquet", f"_{height}m.parquet")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        final_aep.write_parquet(out_path)

        # Resumen por consola
        print("Ranking Top 5 Estaciones (Teórico GWh):")
        top_5 = final_aep.head(5)
        for row in top_5.iter_rows(named=True):
            theo = (
                f"{row['theoretical_aep_gwh']:.2f}"
                if row["theoretical_aep_gwh"] is not None
                else "N/A"
            )
            emp = (
                f"{row['empirical_aep_gwh']:.2f}"
                if row["empirical_aep_gwh"] is not None
                else "N/A"
            )
            print(
                f"  {row['rank']}. {row['station']}: "
                f"{theo} GWh (Empírico: {emp} GWh) "
                f"- k: {row['weibull_k'] or 0:.2f}, A: {row['weibull_A'] or 0:.2f}"
            )

        print(f"  Resultados guardados en: {out_path}")


if __name__ == "__main__":
    run_aep_pipeline()
