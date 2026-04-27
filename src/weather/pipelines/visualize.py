"""Pipeline step: Visualization generation.

Generates interactive wind roses for top stations and an interactive wind potential map.
"""

import os

import polars as pl

from weather.visualization.maps import create_potential_map
from weather.visualization.timeseries import (
    plot_stations_monthly_lines,
    plot_top_stations_timeseries,
)
from weather.visualization.wind_rose import compute_wind_rose_data, plot_wind_rose


def run_visualization_pipeline(
    ts_input_path: str = "data/clean/all_stations_qc.parquet",
    aep_100m_path: str = "data/results/aep_ranking_100m.parquet",
    output_dir: str = "docs/visuals",
) -> None:
    """Execute the visualization generation pipeline.

    Generates:
    - Interactive map with AEP potential across the region
    - Wind roses for the top 3 stations
    """
    print("Iniciando generación de visualizaciones interactivas...")
    os.makedirs(output_dir, exist_ok=True)

    aep_df = pl.DataFrame()
    # 1. Mapa de potencial eólico
    if os.path.exists(aep_100m_path):
        print(f"Generando mapa de potencial a partir de {aep_100m_path}...")
        aep_df = pl.read_parquet(aep_100m_path)
        m = create_potential_map(aep_df, title="AEP Teórico (100m)")
        map_path = os.path.join(output_dir, "mapa_potencial_100m.html")
        m.save(map_path)
        print(f"  Mapa guardado en {map_path}")
    else:
        print(f"ADVERTENCIA: No se encontró {aep_100m_path} para el mapa.")

    # 2. Rosas de viento
    if os.path.exists(ts_input_path) and not aep_df.is_empty():
        print("Generando rosas de viento interactivas (Top 3 estaciones a 100m)...")
        ts_df = pl.read_parquet(ts_input_path)

        # Get top 3 stations
        top_stations = aep_df.sort("rank").head(3)["station"].to_list()

        for station in top_stations:
            print(f"  Procesando rosa para {station}...")
            st_data = ts_df.filter(pl.col("station") == station)

            # Compute data
            rose_data = compute_wind_rose_data(st_data, ws_col="ws100", wd_col="wd100")

            if not rose_data.is_empty():
                fig = plot_wind_rose(
                    rose_data, title=f"Rosa de Vientos a 100m - {station}"
                )
                html_path = os.path.join(output_dir, f"wind_rose_{station}_100m.html")
                fig.write_html(html_path)
                print(f"  Guardada en {html_path}")

        # 3. Series Temporales de Velocidad Media
        print("Generando series temporales de velocidad media...")

        # Combined bar chart
        fig_ts_bar = plot_top_stations_timeseries(ts_df, top_stations, ws_col="ws100")
        ts_bar_path = os.path.join(output_dir, "timeseries_mean_ws_top3.html")
        fig_ts_bar.write_html(ts_bar_path)
        print(f"  Serie temporal (barras combinadas) guardada en {ts_bar_path}")

        # Line chart by station
        fig_ts_lines = plot_stations_monthly_lines(ts_df, top_stations, ws_col="ws100")
        ts_lines_path = os.path.join(output_dir, "timeseries_lines_ws_top3.html")
        fig_ts_lines.write_html(ts_lines_path)
        print(f"  Serie temporal (líneas por estación) guardada en {ts_lines_path}")

    print("Pipeline de visualización completado con éxito.")


if __name__ == "__main__":
    run_visualization_pipeline()
