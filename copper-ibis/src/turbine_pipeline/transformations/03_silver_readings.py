"""Silver: long/narrow sensor readings, split into clean and quarantined.

The quarantine pattern: one temporary view evaluates the rules exactly once, and two
streaming tables read complementary halves of it. Evaluating the rules separately in each
table would risk a row landing in both or neither, because some rules
(``event_ts_not_in_future``) depend on wall-clock time.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from turbine_pipeline.lib.naming import V_SILVER_CANDIDATES, Names
from turbine_pipeline.lib.quality import READING_RULES
from turbine_pipeline.lib.silver import (
    deduplicate_readings,
    quarantined_readings,
    silver_candidates,
    valid_readings,
)

names = Names(spark.conf.get("turbine.catalog"))


@dp.temporary_view(
    # Single-part by requirement: qualifying a temporary view is a hard error, since it
    # does not live in a schema at all.
    name=V_SILVER_CANDIDATES,
    comment="Exploded readings with quality verdict attached. Not published to Unity Catalog.",
)
def v_silver_candidates():
    return silver_candidates(spark.readStream.table(names.bronze_sensor_raw))


@dp.table(
    name=names.silver_sensor_readings,
    comment="Validated sensor readings, one row per (turbine_id, tag, event_ts). "
    "Long/narrow so a new sensor tag needs no schema change.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
# Warn-level expectations: the split to quarantine already removes these rows, so the
# value here is the violation metrics in the pipeline event log, not enforcement.
@dp.expect_all(READING_RULES)
def silver_sensor_readings():
    return deduplicate_readings(valid_readings(spark.readStream.table(V_SILVER_CANDIDATES)))


@dp.table(
    name=names.silver_sensor_quarantine,
    comment="Readings that failed one or more quality rules, with violation_reasons "
    "naming each failure. Nothing is dropped silently.",
    table_properties={"quality": "quarantine"},
)
def silver_sensor_quarantine():
    return quarantined_readings(spark.readStream.table(V_SILVER_CANDIDATES)).withColumn(
        "quarantined_at", F.current_timestamp()
    )
