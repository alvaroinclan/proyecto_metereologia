"""Tests for wind-specific quality control (calm corrections & sector consistency)."""

import numpy as np
import polars as pl
import pytest

from weather.data.quality import (
    apply_calm_corrections,
    compute_sector_frequencies,
    flag_sector_inconsistencies,
    run_quality_control,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_wind_df() -> pl.DataFrame:
    """A small but realistic wind DataFrame with 2 stations and 24 hours.

    station_0 has mostly moderate winds with a few calm readings.
    station_1 has all moderate winds (no calms).
    """
    rng = np.random.default_rng(42)
    n_hours = 24
    stations = ["station_0", "station_1"]

    rows: list[dict] = []
    for st in stations:
        for h in range(n_hours):
            if st == "station_0" and h < 3:
                # First 3 hours are calm
                ws = rng.uniform(0.0, 0.4)
                wd = rng.uniform(0.0, 360.0)  # physically meaningless
            else:
                ws = rng.uniform(2.0, 12.0)
                wd = rng.uniform(0.0, 360.0)
            rows.append({
                "station": st,
                "time": f"2025-01-01T{h:02d}:00:00",
                "ws10": ws,
                "wd10": wd,
            })

    return pl.DataFrame(rows)


@pytest.fixture()
def uniform_wind_df() -> pl.DataFrame:
    """DataFrame with perfectly uniform wind direction distribution.

    Each of 12 sectors gets exactly the same number of observations.
    """
    n_per_sector = 100
    n_sectors = 12
    sector_width = 30.0
    rows: list[dict] = []

    for sector in range(n_sectors):
        mid = sector * sector_width + sector_width / 2
        for _ in range(n_per_sector):
            rows.append({
                "station": "uniform_st",
                "time": "2025-01-01T00:00:00",
                "ws10": 5.0,
                "wd10": mid,
            })

    return pl.DataFrame(rows)


@pytest.fixture()
def biased_wind_df() -> pl.DataFrame:
    """DataFrame with a heavily biased wind direction (sector 0 dominates)."""
    rows: list[dict] = []

    # 500 observations in sector 0 (0–30°)
    for _ in range(500):
        rows.append({
            "station": "biased_st",
            "time": "2025-01-01T00:00:00",
            "ws10": 6.0,
            "wd10": 15.0,
        })

    # 10 observations in every other sector (1..11)
    for sector in range(1, 12):
        mid = sector * 30.0 + 15.0
        for _ in range(10):
            rows.append({
                "station": "biased_st",
                "time": "2025-01-01T00:00:00",
                "ws10": 6.0,
                "wd10": mid,
            })

    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests – Calm Corrections
# ---------------------------------------------------------------------------

class TestApplyCalmCorrections:
    """Tests for apply_calm_corrections."""

    def test_calm_direction_is_nullified(self, sample_wind_df: pl.DataFrame):
        """Directions for calm readings (ws < 0.5) should be set to null."""
        result = apply_calm_corrections(sample_wind_df, calm_threshold=0.5)

        calm_rows = result.filter(pl.col("is_calm"))
        assert calm_rows["wd10"].is_null().all()

    def test_non_calm_direction_preserved(self, sample_wind_df: pl.DataFrame):
        """Non-calm readings should retain their original direction."""
        result = apply_calm_corrections(sample_wind_df, calm_threshold=0.5)

        non_calm = result.filter(~pl.col("is_calm"))
        assert non_calm["wd10"].is_null().sum() == 0

    def test_is_calm_flag_added(self, sample_wind_df: pl.DataFrame):
        result = apply_calm_corrections(sample_wind_df)
        assert "is_calm" in result.columns
        assert result["is_calm"].dtype == pl.Boolean

    def test_calm_count_matches(self, sample_wind_df: pl.DataFrame):
        """station_0 has 3 calm readings; station_1 has 0."""
        result = apply_calm_corrections(sample_wind_df, calm_threshold=0.5)

        st0_calms = result.filter(
            (pl.col("station") == "station_0") & pl.col("is_calm")
        ).height
        assert st0_calms == 3

        st1_calms = result.filter(
            (pl.col("station") == "station_1") & pl.col("is_calm")
        ).height
        assert st1_calms == 0

    def test_zero_threshold_marks_nothing_calm(self, sample_wind_df: pl.DataFrame):
        """With threshold 0, only exactly 0.0 m/s would be calm."""
        result = apply_calm_corrections(sample_wind_df, calm_threshold=0.0)
        # All observations in the fixture are > 0
        assert result["is_calm"].sum() == 0

    def test_high_threshold_marks_everything_calm(self, sample_wind_df: pl.DataFrame):
        """Threshold higher than all speeds → everything is calm."""
        result = apply_calm_corrections(sample_wind_df, calm_threshold=999.0)
        assert result["is_calm"].all()
        assert result["wd10"].is_null().all()

    def test_negative_threshold_raises(self, sample_wind_df: pl.DataFrame):
        with pytest.raises(ValueError, match="calm_threshold must be >= 0"):
            apply_calm_corrections(sample_wind_df, calm_threshold=-1.0)

    def test_missing_column_raises(self):
        df = pl.DataFrame({"a": [1.0]})
        with pytest.raises(ValueError, match="Column 'ws10' not found"):
            apply_calm_corrections(df)

    def test_original_dataframe_unchanged(self, sample_wind_df: pl.DataFrame):
        """Verify immutability: the input DataFrame is not modified."""
        original_wd = sample_wind_df["wd10"].to_list()
        _ = apply_calm_corrections(sample_wind_df)
        assert sample_wind_df["wd10"].to_list() == original_wd
        assert "is_calm" not in sample_wind_df.columns

    def test_custom_column_names(self):
        """Works with non-default wind column names (e.g. ws100 / wd100)."""
        df = pl.DataFrame({
            "station": ["a", "a"],
            "speed": [0.1, 5.0],
            "direction": [180.0, 270.0],
        })
        result = apply_calm_corrections(df, ws_col="speed", wd_col="direction")
        assert result.filter(pl.col("is_calm")).height == 1
        assert result.filter(pl.col("is_calm"))["direction"].is_null().all()


# ---------------------------------------------------------------------------
# Tests – Sector Frequencies
# ---------------------------------------------------------------------------

class TestComputeSectorFrequencies:
    """Tests for compute_sector_frequencies."""

    def test_uniform_frequencies(self, uniform_wind_df: pl.DataFrame):
        """All 12 sectors should have freq ≈ 1/12."""
        freqs = compute_sector_frequencies(uniform_wind_df, n_sectors=12)

        assert freqs.height == 12  # one row per sector
        for row in freqs.iter_rows(named=True):
            assert row["freq"] == pytest.approx(1 / 12, abs=1e-9)

    def test_sectors_cover_full_range(self, sample_wind_df: pl.DataFrame):
        """Sector indices should be in [0, n_sectors)."""
        freqs = compute_sector_frequencies(sample_wind_df, n_sectors=12)
        sectors = freqs["sector"].to_list()
        for s in sectors:
            assert 0 <= s < 12

    def test_freq_sums_to_one_per_station(self, sample_wind_df: pl.DataFrame):
        """Within each station, frequencies should sum to 1."""
        # First apply calm corrections so some dirs are null
        corrected = apply_calm_corrections(sample_wind_df)
        freqs = compute_sector_frequencies(corrected, n_sectors=12)

        for st in freqs["station"].unique().to_list():
            st_freq = freqs.filter(pl.col("station") == st)["freq"].sum()
            assert st_freq == pytest.approx(1.0, abs=1e-9)

    def test_null_directions_excluded(self):
        """Observations with null direction are not counted."""
        df = pl.DataFrame({
            "station": ["s"] * 5,
            "wd10": [10.0, None, 100.0, None, 200.0],
            "ws10": [5.0, 0.1, 5.0, 0.1, 5.0],
        })
        freqs = compute_sector_frequencies(df, n_sectors=12)
        total = freqs["count"].sum()
        assert total == 3  # only non-null dirs counted

    def test_direction_360_maps_to_sector_0(self):
        """Direction exactly 360° should wrap to sector 0."""
        df = pl.DataFrame({
            "station": ["s", "s"],
            "wd10": [0.0, 360.0],
            "ws10": [5.0, 5.0],
        })
        freqs = compute_sector_frequencies(df, n_sectors=12)
        assert freqs.filter(pl.col("sector") == 0)["count"].sum() == 2

    def test_custom_n_sectors(self, uniform_wind_df: pl.DataFrame):
        """4 sectors of 90° each."""
        freqs = compute_sector_frequencies(uniform_wind_df, n_sectors=4)
        # Uniform data → each 90° quadrant gets 3 of the 12 original sectors
        # so each should have freq = 0.25
        for row in freqs.iter_rows(named=True):
            assert row["freq"] == pytest.approx(0.25, abs=1e-9)


# ---------------------------------------------------------------------------
# Tests – Flag Sector Inconsistencies
# ---------------------------------------------------------------------------

class TestFlagSectorInconsistencies:
    """Tests for flag_sector_inconsistencies."""

    def test_uniform_not_flagged(self, uniform_wind_df: pl.DataFrame):
        """A perfectly uniform distribution should not be flagged."""
        freqs = compute_sector_frequencies(uniform_wind_df, n_sectors=12)
        flags = flag_sector_inconsistencies(freqs, n_sectors=12, max_deviation=3.0)

        assert flags.height == 1
        assert not flags["flagged"][0]
        # Chi-squared should be ≈ 0 for a perfect uniform
        assert flags["chi2"][0] == pytest.approx(0.0, abs=1e-6)

    def test_biased_flagged(self, biased_wind_df: pl.DataFrame):
        """A heavily biased direction should be flagged."""
        freqs = compute_sector_frequencies(biased_wind_df, n_sectors=12)
        flags = flag_sector_inconsistencies(freqs, n_sectors=12, max_deviation=3.0)

        assert flags.height == 1
        assert flags["flagged"][0]
        assert flags["chi2"][0] > 0
        assert flags["max_sector_deviation"][0] > 3.0

    def test_max_deviation_column(self, biased_wind_df: pl.DataFrame):
        """The max deviation should reflect the dominant sector."""
        freqs = compute_sector_frequencies(biased_wind_df, n_sectors=12)
        flags = flag_sector_inconsistencies(freqs, n_sectors=12)

        # sector 0 has 500/610 ≈ 0.82 vs expected 1/12 ≈ 0.083 → ratio ≈ 9.8
        assert flags["max_sector_deviation"][0] > 5.0

    def test_lenient_threshold_no_flag(self, biased_wind_df: pl.DataFrame):
        """With a very high tolerance even the biased station isn't flagged."""
        freqs = compute_sector_frequencies(biased_wind_df, n_sectors=12)
        flags = flag_sector_inconsistencies(
            freqs, n_sectors=12, max_deviation=999.0
        )
        assert not flags["flagged"][0]

    def test_multi_station(self, sample_wind_df: pl.DataFrame):
        """Should return one row per station."""
        freqs = compute_sector_frequencies(sample_wind_df, n_sectors=12)
        flags = flag_sector_inconsistencies(freqs, n_sectors=12)

        unique_stations = sample_wind_df["station"].unique().to_list()
        assert flags.height == len(unique_stations)


# ---------------------------------------------------------------------------
# Tests – End-to-end run_quality_control
# ---------------------------------------------------------------------------

class TestRunQualityControl:
    """Tests for the combined run_quality_control pipeline."""

    def test_returns_two_dataframes(self, sample_wind_df: pl.DataFrame):
        df_c, flags = run_quality_control(sample_wind_df)
        assert isinstance(df_c, pl.DataFrame)
        assert isinstance(flags, pl.DataFrame)

    def test_corrected_df_has_is_calm(self, sample_wind_df: pl.DataFrame):
        df_c, _ = run_quality_control(sample_wind_df)
        assert "is_calm" in df_c.columns

    def test_flags_contain_expected_columns(self, sample_wind_df: pl.DataFrame):
        _, flags = run_quality_control(sample_wind_df)
        for col in ("station", "chi2", "max_sector_deviation", "flagged"):
            assert col in flags.columns

    def test_row_count_unchanged(self, sample_wind_df: pl.DataFrame):
        """Calm correction should not drop rows."""
        df_c, _ = run_quality_control(sample_wind_df)
        assert df_c.height == sample_wind_df.height

    def test_all_stations_get_flags(self, sample_wind_df: pl.DataFrame):
        _, flags = run_quality_control(sample_wind_df)
        stations_in = set(sample_wind_df["station"].unique().to_list())
        stations_out = set(flags["station"].to_list())
        assert stations_in == stations_out

    def test_custom_parameters(self, sample_wind_df: pl.DataFrame):
        """Ensure custom parameters are forwarded correctly."""
        df_c, flags = run_quality_control(
            sample_wind_df,
            calm_threshold=2.0,
            n_sectors=8,
            max_deviation=5.0,
        )
        # With threshold=2.0, at least the 3 original calms are caught
        calm_count = df_c.filter(pl.col("is_calm")).height
        assert calm_count >= 3

        # The flags table should still have all stations
        assert flags.height == sample_wind_df["station"].n_unique()

    def test_pipeline_with_ws100_wd100(self):
        """QC pipeline works with 100m height columns."""
        df = pl.DataFrame({
            "station": ["s"] * 10,
            "time": [f"2025-01-01T{h:02d}:00:00" for h in range(10)],
            "ws100": [0.1, 0.2, 5.0, 6.0, 7.0, 8.0, 3.0, 4.0, 9.0, 10.0],
            "wd100": [10.0, 350.0, 90.0, 180.0, 270.0, 45.0, 135.0, 225.0, 315.0, 60.0],
        })
        df_c, flags = run_quality_control(
            df, ws_col="ws100", wd_col="wd100"
        )
        assert "is_calm" in df_c.columns
        # 2 calms (0.1 and 0.2)
        assert df_c.filter(pl.col("is_calm")).height == 2
