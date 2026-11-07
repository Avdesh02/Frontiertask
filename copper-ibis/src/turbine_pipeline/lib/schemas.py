"""Explicit schemas for the raw landing files.

Auto Loader can infer schemas, but an explicit schema makes the contract reviewable, keeps
inference cost out of the streaming path, and means a malformed producer change shows up in
``_rescued_data`` instead of silently widening a column.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# Sensor tags the platform knows about. The silver layer is long/narrow so an unknown tag
# still flows through without a code change -- this list only drives the gold pivot and the
# threshold lookup.
KNOWN_TAGS: tuple[str, ...] = (
    "gear_friction",
    "oil_temperature",
    "blade_velocity",
)

# Device-reported quality flags that we accept into silver.
ACCEPTED_QUALITY_FLAGS: tuple[str, ...] = ("GOOD", "SUSPECT")

# One nested reading inside the per-turbine payload.
READING_STRUCT = StructType(
    [
        StructField("value", DoubleType(), nullable=True),
        StructField("unit", StringType(), nullable=True),
        StructField("quality", StringType(), nullable=True),
    ]
)

# Raw sensor payload: one JSON object per turbine per instant, all tags nested under
# `readings`. A MapType (rather than a StructType of known tags) is what lets a new tag
# appear without a schema change.
SENSOR_PAYLOAD_SCHEMA = StructType(
    [
        StructField("turbine_id", StringType(), nullable=False),
        StructField("site_id", StringType(), nullable=True),
        StructField("event_ts", StringType(), nullable=True),
        StructField("seq", LongType(), nullable=True),
        StructField("readings", MapType(StringType(), READING_STRUCT), nullable=True),
    ]
)

# Raw turbine dimension change events -- the batch side of the pipeline.
TURBINE_CDC_SCHEMA = StructType(
    [
        StructField("turbine_id", StringType(), nullable=False),
        StructField("site_id", StringType(), nullable=True),
        StructField("model", StringType(), nullable=True),
        StructField("manufacturer", StringType(), nullable=True),
        StructField("rated_power_kw", IntegerType(), nullable=True),
        StructField("hub_height_m", IntegerType(), nullable=True),
        StructField("rotor_diameter_m", IntegerType(), nullable=True),
        StructField("commissioned_on", StringType(), nullable=True),
        StructField("operation", StringType(), nullable=True),
        StructField("change_seq", LongType(), nullable=True),
        StructField("changed_at", StringType(), nullable=True),
    ]
)

# Columns dropped on the way into the SCD2 dimension via AUTO CDC's `except_column_list`:
# CDC control columns plus ingest provenance, neither of which is a turbine attribute and
# both of which would otherwise cause a spurious new SCD2 version on every reload.
DIM_CDC_CONTROL_COLUMNS: tuple[str, ...] = (
    "operation",
    "change_seq",
    "_ingested_at",
    "_source_file",
    "_source_file_modified_at",
    "_rescued_data",
)

# Schema of the quarantine side table's violation list.
VIOLATION_ARRAY_TYPE = ArrayType(StringType())
