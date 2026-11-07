"""End-to-end assertions against Unity Catalog after a real pipeline run.

Run order matters: CI deploys the bundle, uploads seed files to the landing Volume, runs
the pipeline to completion, then runs these. Each test states an invariant that can only
hold if data genuinely flowed bronze -> silver -> gold.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration_test


def table(spark, catalog: str, schema: str, name: str):
    return spark.read.table(f"{catalog}.{schema}.{name}")


class TestBronze:
    def test_sensor_rows_were_ingested(self, spark, catalog):
        assert table(spark, catalog, "bronze", "bronze_sensor_raw").count() > 0

    def test_turbine_change_events_were_ingested(self, spark, catalog):
        assert table(spark, catalog, "bronze", "bronze_turbine_cdc").count() > 0

    def test_no_unexpected_fields_were_rescued(self, spark, catalog):
        """A populated _rescued_data means the producer sent something off-contract."""
        df = table(spark, catalog, "bronze", "bronze_sensor_raw")
        rescued = df.where("_rescued_data IS NOT NULL").count()
        assert rescued == 0, (
            f"{rescued} rows had rescued fields -- the source schema has drifted from "
            "SENSOR_PAYLOAD_SCHEMA"
        )


class TestSilver:
    def test_valid_readings_exist(self, spark, catalog):
        assert table(spark, catalog, "silver", "silver_sensor_readings").count() > 0

    def test_quarantine_captured_the_seeded_bad_rows(self, spark, catalog):
        """CI seeds with --bad-rate 0.08, so an empty quarantine means the split broke."""
        assert table(spark, catalog, "silver", "silver_sensor_quarantine").count() > 0

    def test_every_quarantined_row_explains_itself(self, spark, catalog):
        df = table(spark, catalog, "silver", "silver_sensor_quarantine")
        unexplained = df.where("violation_reasons IS NULL OR size(violation_reasons) = 0")
        assert unexplained.count() == 0

    def test_silver_contains_no_invalid_rows(self, spark, catalog):
        """The clean half must really be clean -- this is the core quarantine invariant."""
        df = table(spark, catalog, "silver", "silver_sensor_readings")
        bad = df.where(
            "turbine_id IS NULL OR tag IS NULL OR value IS NULL OR event_ts IS NULL"
        )
        assert bad.count() == 0

    def test_readings_are_unique_at_the_documented_grain(self, spark, catalog):
        df = table(spark, catalog, "silver", "silver_sensor_readings")
        total = df.count()
        distinct = df.select("turbine_id", "tag", "event_ts").distinct().count()
        assert total == distinct, f"{total - distinct} duplicate readings survived dedup"

    def test_long_narrow_shape_covers_the_known_tags(self, spark, catalog):
        df = table(spark, catalog, "silver", "silver_sensor_readings")
        tags = {r.tag for r in df.select("tag").distinct().collect()}
        assert {"gear_friction", "oil_temperature", "blade_velocity"} <= tags


class TestDimension:
    def test_dimension_is_populated(self, spark, catalog):
        assert table(spark, catalog, "silver", "dim_turbine").count() > 0

    def test_scd2_validity_columns_exist(self, spark, catalog):
        columns = set(table(spark, catalog, "silver", "dim_turbine").columns)
        assert {"__START_AT", "__END_AT"} <= columns

    def test_each_turbine_has_exactly_one_current_version(self, spark, catalog):
        """More than one open-ended row per key means the SCD2 merge is broken."""
        df = table(spark, catalog, "silver", "dim_turbine")
        offenders = (
            df.where("__END_AT IS NULL")
            .groupBy("turbine_id")
            .count()
            .where("count > 1")
        )
        assert offenders.count() == 0

    def test_history_was_retained(self, spark, catalog):
        """CI seeds two generations, so at least one turbine must have a closed version."""
        df = table(spark, catalog, "silver", "dim_turbine")
        assert df.where("__END_AT IS NOT NULL").count() > 0

    def test_cdc_control_columns_are_not_in_the_dimension(self, spark, catalog):
        columns = set(table(spark, catalog, "silver", "dim_turbine").columns)
        assert "operation" not in columns
        assert "change_seq" not in columns
        # _ingested_at is current_timestamp(); leaking it here would create a spurious
        # new SCD2 version on every reload.
        assert "_ingested_at" not in columns


class TestGold:
    def test_enriched_fact_is_populated(self, spark, catalog):
        assert table(spark, catalog, "gold", "gold_reading_enriched").count() > 0

    def test_enrichment_did_not_duplicate_readings(self, spark, catalog):
        """An as-of join overlap bug shows up as more gold rows than silver rows."""
        silver = table(spark, catalog, "silver", "silver_sensor_readings").count()
        enriched = table(spark, catalog, "gold", "gold_reading_enriched").count()
        assert enriched == silver, (
            f"gold has {enriched} rows vs silver {silver} -- the SCD2 validity windows "
            "likely overlap"
        )

    def test_most_readings_matched_a_turbine(self, spark, catalog):
        df = table(spark, catalog, "gold", "gold_reading_enriched")
        matched = df.where("dim_model IS NOT NULL").count()
        assert matched > 0, "no reading matched the dimension; check the as-of join bounds"

    def test_window_aggregates_are_populated(self, spark, catalog):
        assert table(spark, catalog, "gold", "gold_tag_window_agg").count() > 0

    def test_aggregate_bounds_are_coherent(self, spark, catalog):
        df = table(spark, catalog, "gold", "gold_tag_window_agg")
        incoherent = df.where(
            "min_value > max_value OR avg_value < min_value OR avg_value > max_value"
        )
        assert incoherent.count() == 0

    def test_current_state_has_one_row_per_turbine(self, spark, catalog):
        df = table(spark, catalog, "gold", "gold_turbine_current_state")
        total = df.count()
        assert total > 0
        assert total == df.select("turbine_id").distinct().count()

    def test_alerts_table_exists_and_is_well_formed(self, spark, catalog):
        df = table(spark, catalog, "gold", "gold_threshold_alerts")
        # An empty alerts table is legitimate if nothing breached, but every row that is
        # there must carry a recognised severity.
        bad = df.where("severity NOT IN ('WARN', 'CRITICAL')")
        assert bad.count() == 0
