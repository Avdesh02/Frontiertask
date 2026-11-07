"""Dataset naming.

Every dataset in this pipeline is referenced by its fully-qualified
``catalog.schema.table`` name, for two reasons:

1. **Bare names resolve against the pipeline's default schema, not against the sibling
   dataset's actual schema.** With a medallion layout spanning `bronze`, `silver`, and
   `gold`, a bare ``spark.readStream.table("silver_sensor_readings")`` would look in the
   pipeline default (`bronze`) and fail the update with a confusing "does not exist".

2. Qualified names are independent of which pipeline's defaults are in effect, so
   splitting this into separate bronze/silver/gold pipelines later is a configuration
   change rather than a code rewrite.

The catalog is injected as a pipeline configuration value rather than hardcoded, so dev
and prod deploy the same source.

One exception: **temporary views must be single-part.** Qualifying one is a hard error
(``TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS``) because a temp view does not live in a schema.
"""

from __future__ import annotations

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"


class Names:
    """Fully-qualified names for every dataset, derived from the runtime catalog."""

    def __init__(self, catalog: str):
        if not catalog:
            raise ValueError(
                "catalog is empty -- the pipeline must set the `turbine.catalog` "
                "configuration value"
            )
        self.catalog = catalog

    def _q(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    # --- bronze ---------------------------------------------------------------------
    @property
    def bronze_sensor_raw(self) -> str:
        return self._q(BRONZE, "bronze_sensor_raw")

    @property
    def bronze_turbine_cdc(self) -> str:
        return self._q(BRONZE, "bronze_turbine_cdc")

    # --- silver ---------------------------------------------------------------------
    @property
    def silver_sensor_readings(self) -> str:
        return self._q(SILVER, "silver_sensor_readings")

    @property
    def silver_sensor_quarantine(self) -> str:
        return self._q(SILVER, "silver_sensor_quarantine")

    @property
    def dim_turbine(self) -> str:
        return self._q(SILVER, "dim_turbine")

    # --- gold -----------------------------------------------------------------------
    @property
    def gold_reading_enriched(self) -> str:
        return self._q(GOLD, "gold_reading_enriched")

    @property
    def gold_tag_window_agg(self) -> str:
        return self._q(GOLD, "gold_tag_window_agg")

    @property
    def gold_threshold_alerts(self) -> str:
        return self._q(GOLD, "gold_threshold_alerts")

    @property
    def gold_turbine_current_state(self) -> str:
        return self._q(GOLD, "gold_turbine_current_state")


#: Temporary views are single-part by requirement, not by choice.
V_SILVER_CANDIDATES = "v_silver_candidates"
