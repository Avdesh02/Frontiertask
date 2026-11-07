"""Quality rules: the predicate logic that decides silver vs quarantine."""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import Row

from turbine_pipeline.lib import quality

# Spark cannot infer a column's type when it is null in every row, which is exactly the
# case these tests construct. Stating the schema avoids CANNOT_DETERMINE_TYPE.
READING_SCHEMA = (
    "turbine_id string, tag string, value double, quality string, event_ts timestamp"
)

CDC_SCHEMA = "turbine_id string, operation string, change_seq long"


@pytest.fixture()
def flagged(spark):
    rows = [
        Row(turbine_id="WTG-0001", tag="oil_temperature", value=70.0, quality="GOOD",
            event_ts=dt.datetime(2026, 11, 7, 10, 0)),
        Row(turbine_id=None, tag="oil_temperature", value=70.0, quality="GOOD",
            event_ts=dt.datetime(2026, 11, 7, 10, 0)),
        Row(turbine_id="WTG-0003", tag="oil_temperature", value=None, quality="GOOD",
            event_ts=dt.datetime(2026, 11, 7, 10, 0)),
        Row(turbine_id="WTG-0004", tag="oil_temperature", value=70.0, quality="NONSENSE",
            event_ts=dt.datetime(2026, 11, 7, 10, 0)),
        Row(turbine_id="WTG-0005", tag="oil_temperature", value=float("nan"), quality="GOOD",
            event_ts=dt.datetime(2026, 11, 7, 10, 0)),
    ]
    df = spark.createDataFrame(rows, schema=READING_SCHEMA)
    return {
        r.turbine_id: r
        for r in quality.with_quality_flags(df, quality.READING_RULES).collect()
    }


def test_clean_row_passes(flagged):
    row = flagged["WTG-0001"]
    assert row.is_valid is True
    assert row.violation_reasons == []


def test_null_key_is_invalid(flagged):
    row = flagged[None]
    assert row.is_valid is False
    assert "turbine_id_present" in row.violation_reasons


def test_null_value_is_invalid(flagged):
    assert "value_present" in flagged["WTG-0003"].violation_reasons


def test_unknown_quality_flag_is_invalid(flagged):
    assert "quality_flag_accepted" in flagged["WTG-0004"].violation_reasons


def test_nan_value_is_invalid(flagged):
    # NaN passes a plain NOT NULL check, which is exactly why there is a separate rule.
    row = flagged["WTG-0005"]
    assert row.is_valid is False
    assert "value_is_finite" in row.violation_reasons


def test_null_predicate_result_counts_as_failure(spark):
    """A rule evaluating to NULL must quarantine the row, never pass it silently."""
    # Explicit schema: event_ts is null in every row, so inference cannot type it.
    df = spark.createDataFrame(
        [Row(turbine_id="WTG-0001", tag="x", value=1.0, quality="GOOD", event_ts=None)],
        schema=READING_SCHEMA,
    )
    row = quality.with_quality_flags(df, quality.READING_RULES).first()
    assert row.is_valid is False
    assert "event_ts_present" in row.violation_reasons


def test_violation_reasons_lists_every_failure(spark):
    df = spark.createDataFrame(
        [Row(turbine_id=None, tag=None, value=None, quality="BAD",
             event_ts=dt.datetime(2026, 11, 7, 10, 0))],
        schema=READING_SCHEMA,
    )
    row = quality.with_quality_flags(df, quality.READING_RULES).first()
    assert set(row.violation_reasons) >= {
        "turbine_id_present",
        "tag_present",
        "value_present",
        "quality_flag_accepted",
    }


def test_cdc_rules_reject_unorderable_rows(spark):
    df = spark.createDataFrame(
        [
            Row(turbine_id="WTG-0001", operation="UPSERT", change_seq=1),
            Row(turbine_id="WTG-0002", operation="UPSERT", change_seq=None),
            Row(turbine_id="WTG-0003", operation="WAT", change_seq=1),
        ],
        schema=CDC_SCHEMA,
    )
    verdicts = {
        r.turbine_id: r.is_valid
        for r in quality.with_quality_flags(df, quality.TURBINE_CDC_RULES).collect()
    }
    assert verdicts["WTG-0001"] is True
    # Without a sequence, AUTO CDC cannot order the change.
    assert verdicts["WTG-0002"] is False
    assert verdicts["WTG-0003"] is False
