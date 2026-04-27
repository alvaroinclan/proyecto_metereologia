"""Tests for Weibull distribution fitting and seasonal variability analysis."""

from datetime import datetime
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest
from scipy.stats import weibull_min as scipy_weibull

from weather.data.weibull import (
    MIN_OBS_FOR_FIT,
    SEASON_MAP,
    SEASONS_ORDER,
    add_season_column,
    compute_seasonal_variability,
    fit_weibull,
    fit_weibull_by_station,
    fit_weibull_by_station_and_season,
    run_weibull_analysis,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def weibull_speeds() -> np.ndarray:
    """Generate 5 000 synthetic wind speeds drawn from a known Weibull.

    shape k = 2.2, scale A = 7.5 m/s  (typical values for a good wind site).
    """
    rng = np.random.default_rng(42)
    return scipy_weibull.rvs(c=2.2, loc=0, scale=7.5, size=5_000, random_state=rng)


@pytest.fixture()
def multi_station_df() -> pl.DataFrame:
    """DataFrame with 3 stations, one full year of hourly data (2025).

    Each station has wind speeds drawn from a different Weibull
    distribution so seasonal analysis results are distinguishable.

    station_0: k=2.0, A=6.0  (moderate site)
    station_1: k=2.5, A=9.0  (windy site)
    station_2: all null speeds (represents an edge/missing station)
    """
    rng = np.random.default_rng(123)
    hours = pl.datetime_range(
        datetime(2025, 1, 1),
        datetime(2025, 12, 31, 23),
        interval="1h",
        eager=True,
    )
    n_hours = len(hours)

    rows: list[dict] = []

    # station_0
    speeds_0 = scipy_weibull.rvs(
        c=2.0, loc=0, scale=6.0, size=n_hours, random_state=rng
    )
    for i, t in enumerate(hours):
        rows.append(
            {
                "station": "station_0",
                "time": t,
                "ws10": float(speeds_0[i]),
                "wd10": float(rng.uniform(0, 360)),
            }
        )

    # station_1
    speeds_1 = scipy_weibull.rvs(
        c=2.5, loc=0, scale=9.0, size=n_hours, random_state=rng
    )
    for i, t in enumerate(hours):
        rows.append(
            {
                "station": "station_1",
                "time": t,
                "ws10": float(speeds_1[i]),
                "wd10": float(rng.uniform(0, 360)),
            }
        )

    # station_2: all null
    for t in hours:
        rows.append({"station": "station_2", "time": t, "ws10": None, "wd10": None})

    return pl.DataFrame(rows)


@pytest.fixture()
def seasonal_df() -> pl.DataFrame:
    """Small DataFrame with exactly 4 seasons, 100 observations each,
    drawn from different Weibull distributions per season.

    Winter (DJF):  k=2.5, A=10.0  (windy)
    Spring (MAM):  k=2.2, A=8.0
    Summer (JJA):  k=1.8, A=5.0   (light winds)
    Autumn (SON):  k=2.0, A=7.0
    """
    rng = np.random.default_rng(99)
    season_params = {
        "DJF": (
            2.5,
            10.0,
            [datetime(2025, 1, 1 + h // 24, h % 24) for h in range(100)],
        ),
        "MAM": (2.2, 8.0, [datetime(2025, 4, 1 + h // 24, h % 24) for h in range(100)]),
        "JJA": (1.8, 5.0, [datetime(2025, 7, 1 + h // 24, h % 24) for h in range(100)]),
        "SON": (
            2.0,
            7.0,
            [datetime(2025, 10, 1 + h // 24, h % 24) for h in range(100)],
        ),
    }

    rows: list[dict] = []
    for _season, (k, a, times) in season_params.items():
        speeds = scipy_weibull.rvs(c=k, loc=0, scale=a, size=100, random_state=rng)
        for i, t in enumerate(times):
            rows.append(
                {
                    "station": "test_st",
                    "time": t,
                    "ws10": float(max(speeds[i], 0.01)),  # ensure positive
                    "wd10": float(rng.uniform(0, 360)),
                }
            )

    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests – fit_weibull (low-level)
# ---------------------------------------------------------------------------


class TestFitWeibull:
    """Tests for the low-level fit_weibull function."""

    def test_known_distribution(self, weibull_speeds: np.ndarray):
        """Parameters recovered from a large sample should be close to true values."""
        result = fit_weibull(weibull_speeds)
        assert result is not None
        k, a = result
        assert k == pytest.approx(2.2, abs=0.15)
        assert a == pytest.approx(7.5, abs=0.5)

    def test_returns_tuple_of_floats(self, weibull_speeds: np.ndarray):
        result = fit_weibull(weibull_speeds)
        assert result is not None
        k, a = result
        assert isinstance(k, float)
        assert isinstance(a, float)

    def test_positive_parameters(self, weibull_speeds: np.ndarray):
        """Both Weibull parameters must be strictly positive."""
        result = fit_weibull(weibull_speeds)
        assert result is not None
        k, a = result
        assert k > 0
        assert a > 0

    def test_too_few_observations(self):
        """Should return None when there aren't enough observations."""
        speeds = np.array([3.0, 4.0, 5.0])  # < MIN_OBS_FOR_FIT
        assert fit_weibull(speeds) is None

    def test_exactly_min_obs(self):
        """Exactly MIN_OBS_FOR_FIT observations should be accepted."""
        rng = np.random.default_rng(7)
        speeds = scipy_weibull.rvs(
            c=2.0, loc=0, scale=6.0, size=MIN_OBS_FOR_FIT, random_state=rng
        )
        result = fit_weibull(speeds)
        assert result is not None

    def test_empty_array(self):
        assert fit_weibull(np.array([])) is None

    def test_uniform_speeds(self):
        """A uniform distribution should still produce valid parameters."""
        rng = np.random.default_rng(0)
        speeds = rng.uniform(1.0, 15.0, size=1000)
        result = fit_weibull(speeds)
        assert result is not None
        k, a = result
        assert k > 0
        assert a > 0

    def test_narrow_distribution(self):
        """Very consistent wind speeds → high k value."""
        # All speeds very close to 8.0 m/s
        speeds = np.full(500, 8.0) + np.random.default_rng(1).normal(0, 0.1, 500)
        speeds = speeds[speeds > 0]
        result = fit_weibull(speeds)
        assert result is not None
        k, _a = result
        # k should be very large for a nearly constant distribution
        assert k > 10

    def test_fit_weibull_exception(self):
        """Should return None when the optimizer raises an exception."""
        speeds = np.array([5.0, 6.0, 7.0] * 10)
        with patch(
            "weather.data.weibull.weibull_min.fit",
            side_effect=RuntimeError("Convergence failed"),
        ):
            assert fit_weibull(speeds) is None


# ---------------------------------------------------------------------------
# Tests – fit_weibull_by_station
# ---------------------------------------------------------------------------


class TestFitWeibullByStation:
    """Tests for station-level annual Weibull fitting."""

    def test_returns_dataframe(self, multi_station_df: pl.DataFrame):
        result = fit_weibull_by_station(multi_station_df)
        assert isinstance(result, pl.DataFrame)

    def test_one_row_per_station(self, multi_station_df: pl.DataFrame):
        result = fit_weibull_by_station(multi_station_df)
        n_stations = multi_station_df["station"].n_unique()
        assert result.height == n_stations

    def test_expected_columns(self, multi_station_df: pl.DataFrame):
        result = fit_weibull_by_station(multi_station_df)
        expected = {"station", "weibull_k", "weibull_A", "mean_ws", "std_ws", "n_obs"}
        assert set(result.columns) == expected

    def test_valid_stations_have_fits(self, multi_station_df: pl.DataFrame):
        """Stations with real data should get valid fit parameters."""
        result = fit_weibull_by_station(multi_station_df)
        for st in ["station_0", "station_1"]:
            row = result.filter(pl.col("station") == st)
            assert row["weibull_k"][0] is not None
            assert row["weibull_A"][0] is not None
            assert row["weibull_k"][0] > 0
            assert row["weibull_A"][0] > 0

    def test_null_station_no_fit(self, multi_station_df: pl.DataFrame):
        """Station with all null data should have None parameters."""
        result = fit_weibull_by_station(multi_station_df)
        row = result.filter(pl.col("station") == "station_2")
        assert row["weibull_k"][0] is None
        assert row["weibull_A"][0] is None
        assert row["n_obs"][0] == 0

    def test_parameters_are_reasonable(self, multi_station_df: pl.DataFrame):
        """Recovered parameters should be in the ballpark of the true values."""
        result = fit_weibull_by_station(multi_station_df)

        # station_0: true k=2.0, A=6.0
        st0 = result.filter(pl.col("station") == "station_0")
        assert st0["weibull_k"][0] == pytest.approx(2.0, abs=0.3)
        assert st0["weibull_A"][0] == pytest.approx(6.0, abs=1.0)

        # station_1: true k=2.5, A=9.0
        st1 = result.filter(pl.col("station") == "station_1")
        assert st1["weibull_k"][0] == pytest.approx(2.5, abs=0.3)
        assert st1["weibull_A"][0] == pytest.approx(9.0, abs=1.0)

    def test_mean_ws_matches(self, multi_station_df: pl.DataFrame):
        """Mean wind speed should match independent calculation."""
        result = fit_weibull_by_station(multi_station_df)
        for st in ["station_0", "station_1"]:
            row = result.filter(pl.col("station") == st)
            independent_mean = multi_station_df.filter(
                (pl.col("station") == st)
                & pl.col("ws10").is_not_null()
                & (pl.col("ws10") > 0)
            )["ws10"].mean()
            assert row["mean_ws"][0] == pytest.approx(independent_mean, rel=1e-6)

    def test_n_obs_positive_for_valid_stations(self, multi_station_df: pl.DataFrame):
        result = fit_weibull_by_station(multi_station_df)
        for st in ["station_0", "station_1"]:
            row = result.filter(pl.col("station") == st)
            assert row["n_obs"][0] > 0

    def test_missing_column_raises(self):
        df = pl.DataFrame({"station": ["a"], "other": [1.0]})
        with pytest.raises(ValueError, match="Column 'ws10' not found"):
            fit_weibull_by_station(df)

    def test_missing_station_col_raises(self):
        df = pl.DataFrame({"ws10": [5.0]})
        with pytest.raises(ValueError, match="Column 'station' not found"):
            fit_weibull_by_station(df)

    def test_custom_column_names(self):
        """Works with non-default column names."""
        rng = np.random.default_rng(42)
        speeds = scipy_weibull.rvs(c=2.0, loc=0, scale=7.0, size=200, random_state=rng)
        df = pl.DataFrame({"loc": ["A"] * 200, "speed": speeds.tolist()})
        result = fit_weibull_by_station(df, ws_col="speed", station_col="loc")
        assert result.height == 1
        assert "loc" in result.columns
        assert result["weibull_k"][0] is not None

    def test_sorted_by_station(self, multi_station_df: pl.DataFrame):
        """Output should be sorted by station name."""
        result = fit_weibull_by_station(multi_station_df)
        stations = result["station"].to_list()
        assert stations == sorted(stations)


# ---------------------------------------------------------------------------
# Tests – add_season_column
# ---------------------------------------------------------------------------


class TestAddSeasonColumn:
    """Tests for the season-tagging utility."""

    def test_adds_season_column(self, multi_station_df: pl.DataFrame):
        result = add_season_column(multi_station_df)
        assert "season" in result.columns

    def test_preserves_original_columns(self, multi_station_df: pl.DataFrame):
        original_cols = set(multi_station_df.columns)
        result = add_season_column(multi_station_df)
        assert original_cols.issubset(set(result.columns))

    def test_preserves_row_count(self, multi_station_df: pl.DataFrame):
        result = add_season_column(multi_station_df)
        assert result.height == multi_station_df.height

    def test_all_months_mapped(self):
        """Every month (1–12) should map to one of the four seasons."""
        rows = [{"time": datetime(2025, m, 15)} for m in range(1, 13)]
        df = pl.DataFrame(rows)
        result = add_season_column(df)

        expected_seasons = [
            "DJF",  # Jan
            "DJF",  # Feb
            "MAM",  # Mar
            "MAM",  # Apr
            "MAM",  # May
            "JJA",  # Jun
            "JJA",  # Jul
            "JJA",  # Aug
            "SON",  # Sep
            "SON",  # Oct
            "SON",  # Nov
            "DJF",  # Dec
        ]
        assert result["season"].to_list() == expected_seasons

    def test_december_is_djf(self):
        df = pl.DataFrame({"time": [datetime(2025, 12, 25)]})
        result = add_season_column(df)
        assert result["season"][0] == "DJF"

    def test_only_four_seasons_present(self, multi_station_df: pl.DataFrame):
        result = add_season_column(multi_station_df)
        unique_seasons = set(result["season"].unique().to_list())
        assert unique_seasons == set(SEASONS_ORDER)

    def test_missing_time_column_raises(self):
        df = pl.DataFrame({"station": ["a"]})
        with pytest.raises(ValueError, match="Column 'time' not found"):
            add_season_column(df)


# ---------------------------------------------------------------------------
# Tests – fit_weibull_by_station_and_season
# ---------------------------------------------------------------------------


class TestFitWeibullByStationAndSeason:
    """Tests for per-station-per-season Weibull fitting."""

    def test_returns_dataframe(self, seasonal_df: pl.DataFrame):
        result = fit_weibull_by_station_and_season(seasonal_df)
        assert isinstance(result, pl.DataFrame)

    def test_expected_columns(self, seasonal_df: pl.DataFrame):
        result = fit_weibull_by_station_and_season(seasonal_df)
        expected = {
            "station",
            "season",
            "weibull_k",
            "weibull_A",
            "mean_ws",
            "std_ws",
            "n_obs",
        }
        assert set(result.columns) == expected

    def test_four_seasons_per_station(self, seasonal_df: pl.DataFrame):
        """Each station should have exactly 4 rows (one per season)."""
        result = fit_weibull_by_station_and_season(seasonal_df)
        n_stations = seasonal_df["station"].n_unique()
        assert result.height == n_stations * 4

    def test_all_seasons_present(self, seasonal_df: pl.DataFrame):
        result = fit_weibull_by_station_and_season(seasonal_df)
        st_seasons = result.filter(pl.col("station") == "test_st")["season"].to_list()
        assert set(st_seasons) == set(SEASONS_ORDER)

    def test_seasonal_parameters_differ(self, seasonal_df: pl.DataFrame):
        """Different seasons should yield different parameters."""
        result = fit_weibull_by_station_and_season(seasonal_df)
        st_fits = result.filter(pl.col("station") == "test_st")

        a_vals = st_fits["weibull_A"].to_list()
        # Not all the same (we drew from different distributions)
        assert len(set(a_vals)) > 1

    def test_winter_has_highest_scale(self, seasonal_df: pl.DataFrame):
        """In our fixture, DJF (A=10) should have the highest scale."""
        result = fit_weibull_by_station_and_season(seasonal_df)
        st_fits = result.filter(pl.col("station") == "test_st")

        djf_a = st_fits.filter(pl.col("season") == "DJF")["weibull_A"][0]
        jja_a = st_fits.filter(pl.col("season") == "JJA")["weibull_A"][0]
        # Winter scale should be larger than summer
        assert djf_a > jja_a

    def test_null_station_all_none(self, multi_station_df: pl.DataFrame):
        """Station with all null data → None for all seasons."""
        result = fit_weibull_by_station_and_season(multi_station_df)
        st2 = result.filter(pl.col("station") == "station_2")
        assert st2["weibull_k"].is_null().all()
        assert st2["weibull_A"].is_null().all()

    def test_n_obs_per_season(self, seasonal_df: pl.DataFrame):
        """Each season should have exactly 100 observations in the fixture."""
        result = fit_weibull_by_station_and_season(seasonal_df)
        for season in SEASONS_ORDER:
            row = result.filter(
                (pl.col("station") == "test_st") & (pl.col("season") == season)
            )
            assert row["n_obs"][0] == 100

    def test_missing_column_raises(self):
        df = pl.DataFrame({"station": ["a"], "time": [datetime(2025, 1, 1)]})
        with pytest.raises(ValueError, match="Column 'ws10' not found"):
            fit_weibull_by_station_and_season(df)

    def test_multi_station_seasonal(self, multi_station_df: pl.DataFrame):
        """Multiple stations should each get 4 seasonal rows."""
        result = fit_weibull_by_station_and_season(multi_station_df)
        n_stations = multi_station_df["station"].n_unique()
        assert result.height == n_stations * 4


# ---------------------------------------------------------------------------
# Tests – compute_seasonal_variability
# ---------------------------------------------------------------------------


class TestComputeSeasonalVariability:
    """Tests for seasonal variability metrics."""

    def test_returns_dataframe(self, seasonal_df: pl.DataFrame):
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)
        assert isinstance(result, pl.DataFrame)

    def test_one_row_per_station(self, seasonal_df: pl.DataFrame):
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)
        assert result.height == 1  # only one station in fixture

    def test_expected_columns(self, seasonal_df: pl.DataFrame):
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)
        expected = {
            "station",
            "cv_k",
            "cv_A",
            "range_k",
            "range_A",
            "best_season",
            "worst_season",
            "n_seasons_fitted",
        }
        assert set(result.columns) == expected

    def test_cv_positive(self, seasonal_df: pl.DataFrame):
        """Coefficient of variation should be positive for varied distributions."""
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)

        assert result["cv_k"][0] > 0
        assert result["cv_A"][0] > 0

    def test_range_positive(self, seasonal_df: pl.DataFrame):
        """Range should be positive when seasons have different parameters."""
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)

        assert result["range_k"][0] > 0
        assert result["range_A"][0] > 0

    def test_best_season_is_djf(self, seasonal_df: pl.DataFrame):
        """In our fixture, DJF (A=10) has the strongest winds."""
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)
        assert result["best_season"][0] == "DJF"

    def test_worst_season_is_jja(self, seasonal_df: pl.DataFrame):
        """In our fixture, JJA (A=5) has the weakest winds."""
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)
        assert result["worst_season"][0] == "JJA"

    def test_n_seasons_fitted_four(self, seasonal_df: pl.DataFrame):
        """All 4 seasons should have been fitted successfully."""
        seasonal_fits = fit_weibull_by_station_and_season(seasonal_df)
        result = compute_seasonal_variability(seasonal_fits)
        assert result["n_seasons_fitted"][0] == 4

    def test_null_station_excluded(self, multi_station_df: pl.DataFrame):
        """Station with no valid fits should not appear in variability output."""
        seasonal_fits = fit_weibull_by_station_and_season(multi_station_df)
        result = compute_seasonal_variability(seasonal_fits)
        station_list = result["station"].to_list()
        assert "station_2" not in station_list

    def test_multi_station_variability(self, multi_station_df: pl.DataFrame):
        """Multiple valid stations should each get a variability row."""
        seasonal_fits = fit_weibull_by_station_and_season(multi_station_df)
        result = compute_seasonal_variability(seasonal_fits)
        # station_0 and station_1 should be present (station_2 is all null)
        assert result.height >= 2

    def test_missing_columns_raises(self):
        df = pl.DataFrame({"station": ["a"], "other": [1.0]})
        with pytest.raises(ValueError, match="Missing columns"):
            compute_seasonal_variability(df)

    def test_all_null_fits_empty_result(self):
        """If all fits failed, variability result should be empty."""
        df = pl.DataFrame(
            {
                "station": ["a", "a", "a", "a"],
                "season": SEASONS_ORDER,
                "weibull_k": [None, None, None, None],
                "weibull_A": [None, None, None, None],
            }
        )
        result = compute_seasonal_variability(df)
        assert result.height == 0

    def test_constant_wind_low_cv(self):
        """Station with identical parameters across seasons → CV ≈ 0."""
        df = pl.DataFrame(
            {
                "station": ["s"] * 4,
                "season": SEASONS_ORDER,
                "weibull_k": [2.0, 2.0, 2.0, 2.0],
                "weibull_A": [7.0, 7.0, 7.0, 7.0],
            }
        )
        result = compute_seasonal_variability(df)
        assert result["cv_k"][0] == pytest.approx(0.0, abs=1e-10)
        assert result["cv_A"][0] == pytest.approx(0.0, abs=1e-10)
        assert result["range_k"][0] == pytest.approx(0.0, abs=1e-10)
        assert result["range_A"][0] == pytest.approx(0.0, abs=1e-10)


# ---------------------------------------------------------------------------
# Tests – run_weibull_analysis (end-to-end)
# ---------------------------------------------------------------------------


class TestRunWeibullAnalysis:
    """Tests for the combined analysis pipeline."""

    def test_returns_three_dataframes(self, multi_station_df: pl.DataFrame):
        annual, seasonal, variability = run_weibull_analysis(multi_station_df)
        assert isinstance(annual, pl.DataFrame)
        assert isinstance(seasonal, pl.DataFrame)
        assert isinstance(variability, pl.DataFrame)

    def test_annual_has_all_stations(self, multi_station_df: pl.DataFrame):
        annual, _, _ = run_weibull_analysis(multi_station_df)
        n_stations = multi_station_df["station"].n_unique()
        assert annual.height == n_stations

    def test_seasonal_has_correct_row_count(self, multi_station_df: pl.DataFrame):
        _, seasonal, _ = run_weibull_analysis(multi_station_df)
        n_stations = multi_station_df["station"].n_unique()
        assert seasonal.height == n_stations * 4

    def test_variability_excludes_null_stations(self, multi_station_df: pl.DataFrame):
        _, _, variability = run_weibull_analysis(multi_station_df)
        # station_2 is all null and should not appear
        assert "station_2" not in variability["station"].to_list()

    def test_consistency_between_outputs(self, multi_station_df: pl.DataFrame):
        """Stations in variability should be a subset of those in annual fits."""
        annual, _, variability = run_weibull_analysis(multi_station_df)
        annual_stations = set(annual["station"].to_list())
        variability_stations = set(variability["station"].to_list())
        assert variability_stations.issubset(annual_stations)

    def test_custom_ws_col(self):
        """Pipeline works with a non-default wind speed column."""
        rng = np.random.default_rng(5)
        n = 200
        df = pl.DataFrame(
            {
                "station": ["s"] * n,
                "time": pl.datetime_range(
                    datetime(2025, 1, 1),
                    datetime(2025, 1, 9, 7),
                    interval="1h",
                    eager=True,
                )[:n],
                "ws100": scipy_weibull.rvs(
                    c=2.0, loc=0, scale=8.0, size=n, random_state=rng
                ).tolist(),
                "wd100": rng.uniform(0, 360, size=n).tolist(),
            }
        )
        annual, seasonal, variability = run_weibull_analysis(df, ws_col="ws100")
        assert annual.height == 1
        assert annual["weibull_k"][0] is not None


# ---------------------------------------------------------------------------
# Tests – SEASON_MAP and SEASONS_ORDER constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Validate the module-level constants."""

    def test_season_map_covers_all_months(self):
        assert set(SEASON_MAP.keys()) == set(range(1, 13))

    def test_season_map_values_valid(self):
        for v in SEASON_MAP.values():
            assert v in SEASONS_ORDER

    def test_seasons_order_length(self):
        assert len(SEASONS_ORDER) == 4

    def test_seasons_order_unique(self):
        assert len(set(SEASONS_ORDER)) == 4
