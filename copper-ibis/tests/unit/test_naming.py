"""Dataset naming.

These run without Spark. They guard the single most dangerous failure mode in a
multi-schema pipeline: an unqualified name silently resolving to the pipeline's default
schema and failing the update with a misleading "dataset does not exist".
"""

from __future__ import annotations

import pytest

from turbine_pipeline.lib.naming import V_SILVER_CANDIDATES, Names

ALL_DATASETS = (
    "bronze_sensor_raw",
    "bronze_turbine_cdc",
    "silver_sensor_readings",
    "silver_sensor_quarantine",
    "dim_turbine",
    "gold_reading_enriched",
    "gold_tag_window_agg",
    "gold_threshold_alerts",
    "gold_turbine_current_state",
)


@pytest.fixture()
def names():
    return Names("wind_dev")


def test_every_dataset_is_three_part(names):
    for dataset in ALL_DATASETS:
        qualified = getattr(names, dataset)
        assert qualified.count(".") == 2, f"{dataset} -> {qualified} is not fully qualified"


def test_every_dataset_uses_the_given_catalog(names):
    for dataset in ALL_DATASETS:
        assert getattr(names, dataset).startswith("wind_dev.")


@pytest.mark.parametrize(
    ("dataset", "expected_schema"),
    [
        ("bronze_sensor_raw", "bronze"),
        ("bronze_turbine_cdc", "bronze"),
        ("silver_sensor_readings", "silver"),
        ("silver_sensor_quarantine", "silver"),
        ("dim_turbine", "silver"),
        ("gold_reading_enriched", "gold"),
        ("gold_tag_window_agg", "gold"),
        ("gold_threshold_alerts", "gold"),
        ("gold_turbine_current_state", "gold"),
    ],
)
def test_dataset_lands_in_the_right_layer(names, dataset, expected_schema):
    assert getattr(names, dataset).split(".")[1] == expected_schema


def test_catalog_is_not_hardcoded():
    """dev and prod must deploy identical source."""
    assert Names("wind_prod").silver_sensor_readings.startswith("wind_prod.")


def test_empty_catalog_is_rejected_early():
    # Better to fail at import with a clear message than to produce ".silver.x".
    with pytest.raises(ValueError, match="catalog is empty"):
        Names("")


def test_temporary_view_name_is_single_part():
    # Qualifying a temp view raises TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS at runtime.
    assert "." not in V_SILVER_CANDIDATES
