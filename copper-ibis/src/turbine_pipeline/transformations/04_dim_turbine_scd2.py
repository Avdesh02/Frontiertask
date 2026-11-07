"""Silver: the turbine dimension as SCD Type 2.

AUTO CDC maintains ``__START_AT`` / ``__END_AT`` validity columns, so a reading taken
before a turbine was re-rated joins to the rating that was in force at the time rather
than today's. ``create_auto_cdc_flow`` is the current API -- the old name was
``apply_changes``.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

from turbine_pipeline.lib.naming import Names
from turbine_pipeline.lib.schemas import DIM_CDC_CONTROL_COLUMNS

names = Names(spark.conf.get("turbine.catalog"))

dp.create_streaming_table(
    name=names.dim_turbine,
    comment="Wind turbine master data, SCD Type 2. __START_AT/__END_AT bound each "
    "version's validity window; __END_AT is NULL on the current row.",
    table_properties={"quality": "silver"},
    # No expectations here: AUTO CDC evaluates a target's expectations against the
    # post-`except_column_list` schema, so any rule mentioning `operation` or
    # `change_seq` would fail to resolve. Those rules live on bronze_turbine_cdc instead.
)

dp.create_auto_cdc_flow(
    # Both sides fully qualified: create_auto_cdc_flow has no separate catalog/schema
    # parameter, so an unqualified target would silently land in the default schema.
    target=names.dim_turbine,
    source=names.bronze_turbine_cdc,
    keys=["turbine_id"],
    # Sequenced by event time, not by the producer's `change_seq`, because __START_AT and
    # __END_AT inherit this column's TYPE. Using the BIGINT change_seq would make the
    # validity columns BIGINT, and the gold as-of join compares them against a TIMESTAMP
    # event_ts. `changed_at` keeps them comparable.
    #
    # The trade-off is that two changes to one turbine within the same millisecond are
    # ordered arbitrarily; `change_seq` would be immune to that. If that matters more than
    # a clean as-of join, sequence by change_seq and cast in enrich_with_dimension instead.
    sequence_by=col("changed_at"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=list(DIM_CDC_CONTROL_COLUMNS),
    stored_as_scd_type=2,
)
