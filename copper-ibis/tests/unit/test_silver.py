"""Silver layer: the nested -> long explode and the valid/quarantine split."""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import Row

from turbine_pipeline.lib import silver
from turbine_pipeline.lib.bronze import standardise_sensor_bronze


def _raw_rows():
    """Two turbines: one fully clean, one with a bad value and a bad quality flag."""
    return [
        Row(
            turbine_id="WTG-0001",
            site_id="SITE-NORTH",
            event_ts="2026-11-07T10:15:30.000Z",
            seq=1,
            readings={
                "gear_friction": Row(value=0.42, unit="coef", quality="GOOD"),
                "oil_temperature": Row(value=78.3, unit="celsius", quality="GOOD"),
            },
        ),
        Row(
            turbine_id="WTG-0002",
            site_id="SITE-SOUTH",
            event_ts="2026-11-07T10:15:31.000Z",
            seq=2,
            readings={
                # Null value -> violates value_present.
                "blade_velocity": Row(value=None, unit="rpm", quality="GOOD"),
                # Unrecognised device flag -> violates quality_flag_accepted.
                "oil_temperature": Row(value=70.0, unit="celsius", quality="BROKEN"),
            },
        ),
    ]


@pytest.fixture()
def candidates(spark):
    raw = spark.createDataFrame(_raw_rows())
    return silver.silver_candidates(standardise_sensor_bronze(raw)).cache()


def test_explode_produces_one_row_per_tag(candidates):
    assert candidates.count() == 4


def test_valid_and_quarantine_are_exact_complements(candidates):
    valid = silver.valid_readings(candidates)
    bad = silver.quarantined_readings(candidates)
    assert valid.count() + bad.count() == candidates.count()
    # No row may appear in both halves.
    overlap = valid.select("turbine_id", "tag").intersect(bad.select("turbine_id", "tag"))
    assert overlap.count() == 0


def test_clean_turbine_passes_entirely(candidates):
    valid = silver.valid_readings(candidates)
    tags = {r.tag for r in valid.where("turbine_id = 'WTG-0001'").collect()}
    assert tags == {"gear_friction", "oil_temperature"}


def test_quarantine_explains_each_violation(candidates):
    bad = {
        (r.turbine_id, r.tag): set(r.violation_reasons)
        for r in silver.quarantined_readings(candidates).collect()
    }
    assert bad[("WTG-0002", "blade_velocity")] == {"value_present"}
    assert bad[("WTG-0002", "oil_temperature")] == {"quality_flag_accepted"}


def test_event_ts_is_parsed_to_timestamp(candidates):
    row = candidates.where("turbine_id = 'WTG-0001'").first()
    assert isinstance(row.event_ts, dt.datetime)


def test_empty_readings_map_still_yields_a_quarantined_row(spark):
    raw = spark.createDataFrame(
        [
            Row(
                turbine_id="WTG-0003",
                site_id="SITE-EAST",
                event_ts="2026-11-07T10:15:32.000Z",
                seq=3,
                readings=None,
            )
        ],
        schema="turbine_id string, site_id string, event_ts string, seq long, "
        "readings map<string, struct<value: double, unit: string, quality: string>>",
    )
    candidates = silver.silver_candidates(standardise_sensor_bronze(raw))
    # explode_outer keeps the row so the loss is visible rather than silent.
    assert candidates.count() == 1
    row = candidates.first()
    assert row.is_valid is False
    assert "tag_present" in row.violation_reasons


def test_future_timestamps_are_quarantined(spark):
    future = (dt.datetime.utcnow() + dt.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    raw = spark.createDataFrame(
        [
            Row(
                turbine_id="WTG-0004",
                site_id="SITE-WEST",
                event_ts=future,
                seq=4,
                readings={"oil_temperature": Row(value=60.0, unit="celsius", quality="GOOD")},
            )
        ]
    )
    candidates = silver.silver_candidates(standardise_sensor_bronze(raw))
    row = candidates.first()
    assert row.is_valid is False
    assert "event_ts_not_in_future" in row.violation_reasons


def test_quality_flag_is_upper_cased(spark):
    raw = spark.createDataFrame(
        [
            Row(
                turbine_id="WTG-0005",
                site_id="SITE-WEST",
                event_ts="2026-11-07T10:15:33.000Z",
                seq=5,
                readings={"oil_temperature": Row(value=60.0, unit="celsius", quality="good")},
            )
        ]
    )
    candidates = silver.silver_candidates(standardise_sensor_bronze(raw))
    row = candidates.first()
    # Lower-case from the device must not be treated as an unknown flag.
    assert row.quality == "GOOD"
    assert row.is_valid is True
