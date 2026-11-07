"""Bronze transforms: land raw files with provenance, change nothing else.

Bronze deliberately does not reshape the nested payload. Keeping the raw structure means a
silver bug can be fixed and replayed from bronze without re-reading the landing volume.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _has_file_metadata(df: DataFrame) -> bool:
    """Whether ``_metadata`` is resolvable on this DataFrame.

    ``_metadata`` is a *hidden* column: it is absent from ``df.schema`` and ``df.columns``
    even on a file-backed source, so the only reliable test is to try to resolve it.
    """
    try:
        df.select(F.col("_metadata.file_path"))
    except Exception:
        return False
    return True


def with_ingest_metadata(df: DataFrame) -> DataFrame:
    """Attach ingest provenance columns.

    Auto Loader exposes source file metadata through the hidden ``_metadata`` column;
    capturing it here is what makes "which file did this bad row come from" answerable
    months later. Falls back to nulls when ``_metadata`` is unavailable, which is the case
    in local unit tests that build DataFrames from in-memory rows.
    """
    out = df.withColumn("_ingested_at", F.current_timestamp())
    if _has_file_metadata(df):
        return out.withColumn("_source_file", F.col("_metadata.file_path")).withColumn(
            "_source_file_modified_at", F.col("_metadata.file_modification_time")
        )
    return out.withColumn("_source_file", F.lit(None).cast("string")).withColumn(
        "_source_file_modified_at", F.lit(None).cast("timestamp")
    )


def standardise_sensor_bronze(df: DataFrame) -> DataFrame:
    """Normalise the raw sensor payload without flattening it.

    Only two changes: parse ``event_ts`` from ISO-8601 text to a real timestamp, and attach
    provenance. The nested ``readings`` map is carried through untouched for silver to explode.
    """
    return with_ingest_metadata(
        df.withColumn("event_ts", F.to_timestamp(F.col("event_ts")))
    )


def standardise_turbine_cdc_bronze(df: DataFrame) -> DataFrame:
    """Normalise the raw turbine dimension CDC feed.

    Parses the two date/time columns and upper-cases ``operation`` so that a producer
    emitting ``"delete"`` does not bypass the delete rule in AUTO CDC.
    """
    return with_ingest_metadata(
        df.withColumn("changed_at", F.to_timestamp(F.col("changed_at")))
        .withColumn("commissioned_on", F.to_date(F.col("commissioned_on")))
        .withColumn("operation", F.upper(F.trim(F.col("operation"))))
    )
