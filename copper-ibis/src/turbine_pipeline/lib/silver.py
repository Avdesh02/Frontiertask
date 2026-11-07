"""Silver transforms: explode the nested payload into long/narrow readings.

The raw feed carries all tags for a turbine in one nested object. Silver turns that into
one row per ``(turbine_id, tag, event_ts)``. Long/narrow is the deliberate choice: a new
sensor tag flows through to silver with zero code change, and only the gold pivot needs
touching if you want it as a named column.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .quality import READING_RULES, with_quality_flags

#: Columns that define a unique reading. Used for dedup and as the documented grain.
READING_GRAIN: tuple[str, ...] = ("turbine_id", "tag", "event_ts")


def explode_readings(df: DataFrame) -> DataFrame:
    """Turn one nested per-turbine payload row into one row per sensor tag.

    ``explode_outer`` rather than ``explode`` so a payload that arrives with an empty or
    null ``readings`` map still produces a row -- it will fail the ``tag_present`` rule and
    land in quarantine, which is the visible outcome we want rather than a vanished record.
    """
    exploded = df.select(
        "turbine_id",
        "site_id",
        "event_ts",
        "seq",
        "_ingested_at",
        "_source_file",
        F.explode_outer("readings").alias("tag", "reading"),
    )
    return exploded.select(
        "turbine_id",
        "site_id",
        "event_ts",
        "seq",
        "tag",
        F.col("reading.value").alias("value"),
        F.col("reading.unit").alias("unit"),
        F.upper(F.col("reading.quality")).alias("quality"),
        "_ingested_at",
        "_source_file",
    )


def silver_candidates(df: DataFrame) -> DataFrame:
    """Exploded readings with quality flags attached, before the valid/quarantine split.

    This is materialised once as a temporary view in the pipeline so that the two
    downstream tables filter complementary halves of the *same* evaluation -- otherwise
    a non-deterministic rule such as the future-timestamp check could let a row fall into
    both tables or neither.
    """
    return with_quality_flags(explode_readings(df), READING_RULES)


def valid_readings(df: DataFrame) -> DataFrame:
    """The clean half: rows passing every rule, with QC columns stripped."""
    return df.where(F.col("is_valid")).drop("is_valid", "violation_reasons")


def quarantined_readings(df: DataFrame) -> DataFrame:
    """The rejected half, keeping ``violation_reasons`` so the failure is self-describing."""
    return df.where(~F.col("is_valid")).drop("is_valid")


def deduplicate_readings(df: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """Drop duplicate readings at the documented grain.

    At-least-once delivery and producer retries both re-send readings. ``dropDuplicates``
    with a watermark bounds the state store -- without the watermark Spark would keep every
    key seen for the lifetime of the stream.
    """
    return df.withWatermark("event_ts", watermark).dropDuplicates(list(READING_GRAIN))
