"""Gold: the four serving datasets.

Thresholds come from the pipeline's ``configuration`` block so dev and prod can use
different limits without a code change.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.gold import (
    current_state,
    enrich_with_dimension,
    tag_window_aggregates,
    threshold_alerts,
    thresholds_from_config,
)
from turbine_pipeline.lib.naming import Names

names = Names(spark.conf.get("turbine.catalog"))

# `spark.conf.get` raises on a missing key unless given a default, so adapt it to the
# (key, default) signature `thresholds_from_config` expects.
THRESHOLDS = thresholds_from_config(lambda key, default: spark.conf.get(key, default))

WINDOW_DURATION = spark.conf.get("turbine.gold.window_duration", "5 minutes")


@dp.table(
    name=names.gold_reading_enriched,
    comment="Every validated reading joined to the turbine attributes that were in force "
    "at the reading's event time (SCD2 as-of join). Full-grain fact for ad-hoc analysis.",
    table_properties={"quality": "gold"},
)
def gold_reading_enriched():
    return enrich_with_dimension(
        spark.readStream.table(names.silver_sensor_readings),
        spark.read.table(names.dim_turbine),
    )


@dp.table(
    name=names.gold_threshold_alerts,
    comment="Readings breaching their tag's warn or critical limit. Limits come from the "
    "pipeline configuration, so they differ per environment without a redeploy.",
    table_properties={"quality": "gold"},
)
def gold_threshold_alerts():
    return threshold_alerts(spark.readStream.table(names.silver_sensor_readings), THRESHOLDS)


@dp.materialized_view(
    name=names.gold_tag_window_agg,
    comment="Tumbling-window min/max/avg/stddev per turbine per tag -- the operational "
    "rollup for dashboards.",
    table_properties={"quality": "gold"},
)
def gold_tag_window_agg():
    return tag_window_aggregates(
        spark.read.table(names.silver_sensor_readings), WINDOW_DURATION
    )


@dp.materialized_view(
    name=names.gold_turbine_current_state,
    comment="Latest value per turbine per tag, pivoted wide for a live status board.",
    table_properties={"quality": "gold"},
)
def gold_turbine_current_state():
    return current_state(spark.read.table(names.silver_sensor_readings))
