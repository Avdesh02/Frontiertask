"""Bronze: turbine dimension change events (the batch side of the pipeline).

Auto Loader is used here too even though the feed is batch -- it gives exactly-once file
tracking for free, so re-dropping a file that was already processed is a no-op.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.bronze import standardise_turbine_cdc_bronze
from turbine_pipeline.lib.naming import Names
from turbine_pipeline.lib.quality import TURBINE_CDC_RULES
from turbine_pipeline.lib.schemas import TURBINE_CDC_SCHEMA

names = Names(spark.conf.get("turbine.catalog"))
TURBINE_LANDING_PATH = spark.conf.get("turbine.landing.turbines_path")


@dp.table(
    name=names.bronze_turbine_cdc,
    comment="Raw turbine master-data change events as landed. Feeds the SCD2 dimension.",
    table_properties={"quality": "bronze"},
)
# Enforced here rather than on dim_turbine: AUTO CDC evaluates a target's expectations
# against the TARGET schema, which is post-`except_column_list` and so no longer has
# `operation` or `change_seq`. Validating at ingestion also fails faster and keeps the
# unorderable rows out of the CDC stream entirely.
@dp.expect_all_or_drop(TURBINE_CDC_RULES)
def bronze_turbine_cdc():
    return standardise_turbine_cdc_bronze(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(TURBINE_CDC_SCHEMA)
        .load(TURBINE_LANDING_PATH)
    )
