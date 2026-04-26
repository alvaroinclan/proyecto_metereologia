import numpy as np
import polars as pl
import pytest

from weather.data.aep import (
    _calculate_aep_vectorized_cdf,
    compute_empirical_aep,
    compute_theoretical_aep,
    get_reference_power_curve,
    rank_locations,
)


def test_reference_power_curve():
    pc = get_reference_power_curve()
    assert pc.height > 0
    assert "wind_speed" in pc.columns
    assert "power_kw" in pc.columns

    # Check cut-in, rated, cut-out logic
    # Under cut-in (4 m/s) should be 0
    assert pc.filter(pl.col("wind_speed") == 3.0)["power_kw"][0] == 0.0
    # Rated speed and above (15-25 m/s) should be 2000
    assert pc.filter(pl.col("wind_speed") == 15.0)["power_kw"][0] == 2000.0
    assert pc.filter(pl.col("wind_speed") == 25.0)["power_kw"][0] == 2000.0
    # Above cut-out (> 25 m/s) should be 0
    assert pc.filter(pl.col("wind_speed") == 26.0)["power_kw"][0] == 0.0


def test_calculate_aep_vectorized_cdf():
    k_vals = np.array([2.0, np.nan])
    a_vals = np.array([8.0, 8.0])
    v_vals = np.array([0.0, 5.0, 10.0, 15.0, 20.0])
    p_vals = np.array([0.0, 100.0, 1000.0, 2000.0, 2000.0])

    aep = _calculate_aep_vectorized_cdf(k_vals, a_vals, v_vals, p_vals)

    assert len(aep) == 2
    assert aep[0] > 0  # Should yield positive GWh
    assert np.isnan(aep[1])  # Null inputs propagate


def test_compute_theoretical_aep():
    weibull_df = pl.DataFrame(
        {
            "station": ["st1", "st2", "st3"],
            "weibull_k": [2.0, 2.2, None],
            "weibull_A": [7.5, 8.5, None],
        }
    )

    pc = get_reference_power_curve()

    res = compute_theoretical_aep(weibull_df, pc)

    assert "theoretical_aep_gwh" in res.columns
    assert res.height == 3

    # st1 and st2 should have valid AEPs
    assert res.filter(pl.col("station") == "st1")["theoretical_aep_gwh"][0] > 0

    # st2 with higher scale A should generally yield higher AEP than st1
    aep_st1 = res.filter(pl.col("station") == "st1")["theoretical_aep_gwh"][0]
    aep_st2 = res.filter(pl.col("station") == "st2")["theoretical_aep_gwh"][0]
    assert aep_st2 > aep_st1

    # st3 should have null AEP due to missing parameters
    assert res.filter(pl.col("station") == "st3")["theoretical_aep_gwh"].is_null()[0]

    # Validate missing columns raise ValueError
    with pytest.raises(ValueError):
        compute_theoretical_aep(weibull_df.drop("weibull_k"), pc)
    with pytest.raises(ValueError):
        compute_theoretical_aep(weibull_df, pc.drop("power_kw"))


def test_compute_empirical_aep():
    ts_df = pl.DataFrame(
        {
            "station": ["st1"] * 1000 + ["st2"] * 1000,
            "ws10": np.concatenate(
                [np.random.normal(8.0, 2.0, 1000), np.random.normal(9.0, 2.0, 1000)]
            ),
        }
    )

    # Prevent negative speeds
    ts_df = ts_df.with_columns(
        pl.when(pl.col("ws10") < 0).then(0).otherwise(pl.col("ws10")).alias("ws10")
    )

    pc = get_reference_power_curve()

    res = compute_empirical_aep(ts_df, pc, ws_col="ws10")

    assert "empirical_aep_gwh" in res.columns
    assert res.height == 2

    # Empirical AEP should be calculated for both
    assert res.filter(pl.col("station") == "st1")["empirical_aep_gwh"][0] > 0
    assert res.filter(pl.col("station") == "st2")["empirical_aep_gwh"][0] > 0

    with pytest.raises(ValueError):
        compute_empirical_aep(ts_df.drop("ws10"), pc, ws_col="ws10")


def test_rank_locations():
    aep_df = pl.DataFrame(
        {"station": ["A", "B", "C"], "theoretical_aep_gwh": [10.5, 25.1, 18.3]}
    )

    ranked = rank_locations(aep_df)

    assert "rank" in ranked.columns
    # B (25.1) should be 1st, C (18.3) 2nd, A (10.5) 3rd
    assert ranked.row(0, named=True)["station"] == "B"
    assert ranked.row(0, named=True)["rank"] == 1
    assert ranked.row(1, named=True)["station"] == "C"
    assert ranked.row(2, named=True)["station"] == "A"

    with pytest.raises(ValueError):
        rank_locations(aep_df, aep_col="missing_col")
