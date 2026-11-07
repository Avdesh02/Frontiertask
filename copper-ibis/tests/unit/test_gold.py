"""Gold layer: thresholds, alerts, aggregates, current state, and the SCD2 as-of join."""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import Row

from turbine_pipeline.lib import gold


def _ts(minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(2026, 11, 7, 10, minute, second)


@pytest.fixture()
def readings(spark):
    return spark.createDataFrame(
        [
            # Normal.
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=70.0,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(0),
            ),
            # Above warn_max (85) but below critical_max (95).
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=88.0,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(1),
            ),
            # Above critical_max.
            Row(
                turbine_id="WTG-0002",
                site_id="SITE-SOUTH",
                tag="oil_temperature",
                value=99.0,
                unit="celsius",
                quality="SUSPECT",
                event_ts=_ts(2),
            ),
            # Below critical_min (-10).
            Row(
                turbine_id="WTG-0003",
                site_id="SITE-EAST",
                tag="oil_temperature",
                value=-20.0,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(3),
            ),
            # Blade velocity has an upper bound only.
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="blade_velocity",
                value=25.0,
                unit="rpm",
                quality="GOOD",
                event_ts=_ts(4),
            ),
        ]
    )


class TestThresholdsFromConfig:
    def test_falls_back_to_defaults_when_unset(self):
        resolved = gold.thresholds_from_config(lambda key, default: default)
        assert resolved["oil_temperature"].critical_max == 95.0
        assert resolved["gear_friction"].warn_max == 0.55

    def test_config_overrides_default(self):
        overrides = {"turbine.threshold.oil_temperature.critical_max": "100.0"}
        resolved = gold.thresholds_from_config(lambda key, default: overrides.get(key, default))
        assert resolved["oil_temperature"].critical_max == 100.0
        # An unset bound must keep its default rather than becoming None.
        assert resolved["oil_temperature"].warn_max == 85.0

    def test_blank_config_value_is_treated_as_unset(self):
        overrides = {"turbine.threshold.oil_temperature.critical_max": "  "}
        resolved = gold.thresholds_from_config(lambda key, default: overrides.get(key, default))
        assert resolved["oil_temperature"].critical_max == 95.0


class TestThresholdAlerts:
    @pytest.fixture()
    def alerts(self, readings):
        thresholds = gold.thresholds_from_config(lambda key, default: default)
        return {
            (r.turbine_id, r.tag): r.severity
            for r in gold.threshold_alerts(readings, thresholds).collect()
        }

    def test_normal_reading_produces_no_alert(self, alerts):
        # WTG-0001's 70.0 reading is in range; only its 88.0 breach should appear.
        assert alerts.get(("WTG-0001", "oil_temperature")) == "WARN"

    def test_critical_outranks_warn(self, alerts):
        assert alerts[("WTG-0002", "oil_temperature")] == "CRITICAL"

    def test_lower_bound_breach_is_detected(self, alerts):
        assert alerts[("WTG-0003", "oil_temperature")] == "CRITICAL"

    def test_tag_with_only_upper_bound(self, alerts):
        assert alerts[("WTG-0001", "blade_velocity")] == "CRITICAL"

    def test_only_breaching_rows_are_emitted(self, readings):
        thresholds = gold.thresholds_from_config(lambda key, default: default)
        result = gold.threshold_alerts(readings, thresholds)
        # 5 input rows, 4 of which breach.
        assert result.count() == 4


class TestWindowAggregates:
    def test_groups_by_window_turbine_and_tag(self, spark):
        rows = [
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=v,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(0, s),
            )
            for v, s in [(70.0, 0), (80.0, 10), (90.0, 20)]
        ]
        df = spark.createDataFrame(rows)
        result = gold.tag_window_aggregates(df, "5 minutes").collect()
        assert len(result) == 1
        agg = result[0]
        assert agg.reading_count == 3
        assert agg.min_value == 70.0
        assert agg.max_value == 90.0
        assert agg.avg_value == pytest.approx(80.0)

    def test_counts_suspect_readings(self, spark):
        rows = [
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=70.0,
                unit="celsius",
                quality=q,
                event_ts=_ts(0, s),
            )
            for q, s in [("GOOD", 0), ("SUSPECT", 1), ("SUSPECT", 2)]
        ]
        result = gold.tag_window_aggregates(spark.createDataFrame(rows)).first()
        assert result.suspect_count == 2

    def test_separate_windows_for_distant_readings(self, spark):
        rows = [
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=70.0,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(m),
            )
            for m in (0, 30)
        ]
        result = gold.tag_window_aggregates(spark.createDataFrame(rows), "5 minutes")
        assert result.count() == 2


class TestCurrentState:
    def test_picks_latest_value_per_tag(self, spark):
        rows = [
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=70.0,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(0),
            ),
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=85.0,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(5),
            ),
            Row(
                turbine_id="WTG-0001",
                site_id="SITE-NORTH",
                tag="blade_velocity",
                value=12.0,
                unit="rpm",
                quality="GOOD",
                event_ts=_ts(3),
            ),
        ]
        result = gold.current_state(spark.createDataFrame(rows)).first()
        assert result.latest_oil_temperature == 85.0
        assert result.latest_blade_velocity == 12.0
        # No gear_friction reading at all -> column present but null.
        assert result.latest_gear_friction is None
        assert result.last_seen_ts == _ts(5)

    def test_one_row_per_turbine(self, spark):
        rows = [
            Row(
                turbine_id=tid,
                site_id="SITE-NORTH",
                tag="oil_temperature",
                value=70.0,
                unit="celsius",
                quality="GOOD",
                event_ts=_ts(0),
            )
            for tid in ("WTG-0001", "WTG-0002", "WTG-0003")
        ]
        assert gold.current_state(spark.createDataFrame(rows)).count() == 3


class TestEnrichWithDimension:
    """The as-of join is the whole reason the dimension is SCD Type 2."""

    @pytest.fixture()
    def dim(self, spark):
        # WTG-0001 was re-rated at 10:02: 4200 kW before, 4400 kW after.
        return spark.createDataFrame(
            [
                Row(
                    turbine_id="WTG-0001",
                    site_id="SITE-NORTH",
                    model="V150-4.2",
                    rated_power_kw=4200,
                    __START_AT=_ts(0),
                    __END_AT=_ts(2),
                ),
                Row(
                    turbine_id="WTG-0001",
                    site_id="SITE-NORTH",
                    model="V150-4.4",
                    rated_power_kw=4400,
                    __START_AT=_ts(2),
                    __END_AT=None,
                ),
            ]
        )

    def test_reading_joins_to_version_valid_at_event_time(self, spark, dim):
        readings = spark.createDataFrame(
            [
                Row(turbine_id="WTG-0001", site_id="SITE-NORTH", tag="oil_temperature",
                    value=70.0, event_ts=_ts(1)),
                Row(turbine_id="WTG-0001", site_id="SITE-NORTH", tag="oil_temperature",
                    value=72.0, event_ts=_ts(3)),
            ]
        )
        by_time = {
            r.event_ts: r.dim_rated_power_kw
            for r in gold.enrich_with_dimension(readings, dim).collect()
        }
        assert by_time[_ts(1)] == 4200, "early reading must see the old rating"
        assert by_time[_ts(3)] == 4400, "later reading must see the new rating"

    def test_each_reading_matches_exactly_one_version(self, spark, dim):
        readings = spark.createDataFrame(
            [
                Row(turbine_id="WTG-0001", site_id="SITE-NORTH", tag="oil_temperature",
                    value=70.0, event_ts=_ts(m))
                for m in (0, 1, 2, 3, 10)
            ]
        )
        # A boundary bug in the validity window would duplicate or drop rows here.
        assert gold.enrich_with_dimension(readings, dim).count() == 5

    def test_unknown_turbine_is_kept_with_null_attributes(self, spark, dim):
        readings = spark.createDataFrame(
            [
                Row(turbine_id="WTG-9999", site_id="SITE-WEST", tag="oil_temperature",
                    value=70.0, event_ts=_ts(1))
            ]
        )
        # Left join: telemetry from a turbine missing from master data must not vanish.
        result = gold.enrich_with_dimension(readings, dim).collect()
        assert len(result) == 1
        assert result[0].dim_rated_power_kw is None
