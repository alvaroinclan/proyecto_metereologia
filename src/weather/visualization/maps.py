"""
Interactive maps using Folium for Wind Potential Visualization.
"""

import branca.colormap as cm
import folium
import numpy as np
import polars as pl


def attach_coordinates(df: pl.DataFrame, station_col: str = "station") -> pl.DataFrame:
    """
    Recreates the grid coordinates used in Fase 1 and attaches them to the results.
    """
    lats = np.arange(42.8, 43.3, 0.1)
    lons = np.arange(-6.0, -5.0, 0.1)
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    lon_flat = lon_grid.flatten()
    lat_flat = lat_grid.flatten()

    mapping = pl.DataFrame(
        {
            station_col: [f"station_{i}" for i in range(len(lon_flat))],
            "latitude": lat_flat,
            "longitude": lon_flat,
        }
    )

    return df.join(mapping, on=station_col, how="left")


def create_potential_map(
    aep_df: pl.DataFrame,
    value_col: str = "theoretical_aep_gwh",
    title: str = "Potencial Eólico",
) -> folium.Map:
    """
    Creates an interactive map coloring stations by their wind potential.
    """
    if "latitude" not in aep_df.columns or "longitude" not in aep_df.columns:
        aep_df = attach_coordinates(aep_df)

    valid = aep_df.filter(
        pl.col("latitude").is_not_null() & pl.col(value_col).is_not_null()
    )
    pd_df = valid.to_pandas()

    if len(pd_df) == 0:
        return folium.Map(location=[43.0, -5.5], zoom_start=8)

    center_lat = pd_df["latitude"].mean()
    center_lon = pd_df["longitude"].mean()

    m = folium.Map(
        location=[center_lat, center_lon], zoom_start=9, tiles="CartoDB positron"
    )

    min_val = pd_df[value_col].min()
    max_val = pd_df[value_col].max()

    colormap = cm.LinearColormap(
        colors=["blue", "cyan", "green", "yellow", "red"],
        vmin=min_val,
        vmax=max_val,
        caption=f"{title} (GWh)",
    )

    m.add_child(colormap)

    for _, row in pd_df.iterrows():
        val = row[value_col]
        color = colormap(val)

        popup_html = f"<b>{row['station']}</b><br>"
        popup_html += f"{title}: {val:.2f} GWh<br>"

        # Display other useful stats if available
        if "empirical_aep_gwh" in row and not np.isnan(row["empirical_aep_gwh"]):
            popup_html += f"Empírico: {row['empirical_aep_gwh']:.2f} GWh<br>"
        if "weibull_A" in row and not np.isnan(row["weibull_A"]):
            popup_html += f"Escala A: {row['weibull_A']:.2f} m/s<br>"
        if "weibull_k" in row and not np.isnan(row["weibull_k"]):
            popup_html += f"Forma k: {row['weibull_k']:.2f}<br>"
        if "rank" in row and not np.isnan(row["rank"]):
            popup_html += f"Ranking: #{int(row['rank'])}<br>"

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=12,
            color="black",
            weight=1,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row['station']}: {val:.2f} GWh",
        ).add_to(m)

    return m
