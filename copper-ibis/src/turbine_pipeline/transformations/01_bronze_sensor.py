"""Bronze: raw sensor telemetry landed by Auto Loader.

Thin wrapper -- all reshaping lives in ``turbine_pipeline.lib.bronze`` so it can be unit
tested without a pipeline runtime.
"""

from pyspark import pipelines as dp

from turbine_pipeline.lib.bronze import standardise_sensor_bronze
from turbine_pipeline.lib.naming import Names
from turbine_pipeline.lib.schemas import SENSOR_PAYLOAD_SCHEMA

names = Names(spark.conf.get("turbine.catalog"))
SENSOR_LANDING_PATH = spark.conf.get("turbine.landing.sensors_path")


@dp.table(
    # Fully qualified: a bare name would land in the pipeline's default schema rather
    # than in bronze. See lib/naming.py.
    name=names.bronze_sensor_raw,
    comment="Raw wind turbine sensor payloads as landed, one row per turbine per instant "
    "with all tags still nested. Replay source for silver.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def bronze_sensor_raw():
    return standardise_sensor_bronze(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        # Anything the producer adds that is not in our schema is preserved here rather
        # than dropped, so a contract change is detectable instead of silent.
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SENSOR_PAYLOAD_SCHEMA)
        .load(SENSOR_LANDING_PATH)
    )
