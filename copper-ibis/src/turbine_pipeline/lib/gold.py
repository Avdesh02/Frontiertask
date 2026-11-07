"""Gold transforms: aggregates, alerts, enrichment, and current state.

Note on pivoting: ``DataFrame.pivot()`` is **not permitted** inside Lakeflow dataset code
(it triggers an eager query to discover the distinct pivot values). Everywhere a wide shape
is needed, this module uses conditional aggregation -- ``max(when(tag = 'x', value))`` --
over the known tag list instead, which is a pure declarative expression.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schemas import KNOWN_TAGS


@dataclass(frozen=True)
class TagThresholds:
    """Warn/critical limits for a single sensor tag.

    Either bound may be ``None`` -- blade velocity has a meaningful upper limit but no
    lower one, for instance.
    """

    tag: str
    warn_min: float | None = None
    warn_max: float | None = None
    critical_min: float | None = None
    critical_max: float | None = None


#: Fallback limits used when the pipeline configuration supplies nothing. Real values come
#: from the bundle's pipeline `configuration` block so dev and prod can differ without a
#: code change -- see `thresholds_from_config`.
DEFAULT_THRESHOLDS: dict[str, TagThresholds] = {
    "gear_friction": TagThresholds("gear_friction", warn_max=0.55, critical_max=0.75),
    "oil_temperature": TagThresholds(
        "oil_temperature", warn_min=5.0, warn_max=85.0, critical_min=-10.0, critical_max=95.0
    ),
    "blade_velocity": TagThresholds("blade_velocity", warn_max=18.0, critical_max=22.0),
}

#: Prefix for threshold keys in the pipeline configuration, e.g.
#: ``turbine.threshold.oil_temperature.critical_max``.
CONFIG_PREFIX = "turbine.threshold"


def thresholds_from_config(
    get_conf, tags: tuple[str, ...] = KNOWN_TAGS
) -> dict[str, TagThresholds]:
    """Build the threshold table from pipeline configuration, falling back to defaults.

    ``get_conf`` is a callable taking ``(key, default)`` -- in the pipeline that is
    ``spark.conf.get``; in tests it is a dict lookup. Passing it in rather than importing
    a SparkSession is what keeps this function unit-testable.
    """
    resolved: dict[str, TagThresholds] = {}
    for tag in tags:
        fallback = DEFAULT_THRESHOLDS.get(tag, TagThresholds(tag))
        bounds: dict[str, float | None] = {}
        for bound in ("warn_min", "warn_max", "critical_min", "critical_max"):
            raw = get_conf(f"{CONFIG_PREFIX}.{tag}.{bound}", None)
            if raw is None or str(raw).strip() == "":
                bounds[bound] = getattr(fallback, bound)
            else:
                bounds[bound] = float(raw)
        resolved[tag] = TagThresholds(tag=tag, **bounds)
    return resolved


def _breach_severity(thresholds: dict[str, TagThresholds]) -> Column:
    """A Column evaluating to ``'CRITICAL'``, ``'WARN'``, or NULL for each reading.

    Critical is checked first so a reading breaching both bounds reports the worse one.
    """
    critical = F.lit(False)
    warn = F.lit(False)
    for tag, limits in thresholds.items():
        is_tag = F.col("tag") == F.lit(tag)
        if limits.critical_max is not None:
            critical = critical | (is_tag & (F.col("value") > F.lit(limits.critical_max)))
        if limits.critical_min is not None:
            critical = critical | (is_tag & (F.col("value") < F.lit(limits.critical_min)))
        if limits.warn_max is not None:
            warn = warn | (is_tag & (F.col("value") > F.lit(limits.warn_max)))
        if limits.warn_min is not None:
            warn = warn | (is_tag & (F.col("value") < F.lit(limits.warn_min)))
    return (
        F.when(critical, F.lit("CRITICAL"))
        .when(warn, F.lit("WARN"))
        .otherwise(F.lit(None).cast("string"))
    )


def threshold_alerts(df: DataFrame, thresholds: dict[str, TagThresholds]) -> DataFrame:
    """Readings that breach their tag's configured limits.

    Emits only breaching rows -- this is an alert feed, not an annotated copy of silver.
    """
    severity = _breach_severity(thresholds)
    annotated = df.withColumn("severity", severity)
    return annotated.where(F.col("severity").isNotNull()).select(
        "turbine_id",
        "site_id",
        "tag",
        "value",
        "unit",
        "event_ts",
        "severity",
        F.current_timestamp().alias("detected_at"),
    )


def tag_window_aggregates(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """Tumbling-window statistics per turbine per tag.

    The standard operational rollup -- this is what a dashboard charts. A watermark is
    applied only on a streaming input, where it bounds the aggregation state; on a batch
    input (a materialized view recomputing from the full table) it would be meaningless.
    """
    source = df.withWatermark("event_ts", "15 minutes") if df.isStreaming else df
    return (
        source.groupBy(
            F.window("event_ts", window_duration).alias("time_window"),
            "turbine_id",
            "site_id",
            "tag",
        )
        .agg(
            F.count("*").alias("reading_count"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.avg("value").alias("avg_value"),
            F.stddev("value").alias("stddev_value"),
            F.sum(F.when(F.col("quality") == F.lit("SUSPECT"), 1).otherwise(0)).alias(
                "suspect_count"
            ),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "turbine_id",
            "site_id",
            "tag",
            "reading_count",
            "min_value",
            "max_value",
            "avg_value",
            "stddev_value",
            "suspect_count",
        )
    )


def current_state(df: DataFrame, tags: tuple[str, ...] = KNOWN_TAGS) -> DataFrame:
    """Latest value per turbine per tag, pivoted wide for a live status board.

    Implemented with conditional aggregation rather than ``.pivot()``, which Lakeflow
    forbids in dataset code. ``max_by(value, event_ts)`` picks the newest reading for each
    tag without needing a window function or a self-join.
    """
    tag_columns = [
        F.max_by(
            F.when(F.col("tag") == F.lit(tag), F.col("value")),
            F.when(F.col("tag") == F.lit(tag), F.col("event_ts")),
        ).alias(f"latest_{tag}")
        for tag in tags
    ]
    tag_time_columns = [
        F.max(F.when(F.col("tag") == F.lit(tag), F.col("event_ts"))).alias(f"{tag}_ts")
        for tag in tags
    ]
    return df.groupBy("turbine_id", "site_id").agg(
        F.max("event_ts").alias("last_seen_ts"),
        *tag_columns,
        *tag_time_columns,
    )


def enrich_with_dimension(readings: DataFrame, dim: DataFrame) -> DataFrame:
    """As-of join each reading to the SCD2 dimension row valid at reading time.

    This is the point of SCD Type 2: a reading from before a turbine was re-rated joins to
    the *old* rating, not today's. ``__START_AT``/``__END_AT`` are the validity columns
    AUTO CDC maintains; ``__END_AT`` is NULL on the current row, hence the ``isNull`` arm.

    The validity columns take their type from the flow's ``sequence_by`` column, so that
    must be a timestamp for these comparisons against ``event_ts`` to resolve. See
    ``transformations/04_dim_turbine_scd2.py``.
    """
    dim_cols = [c for c in dim.columns if c not in {"turbine_id", "site_id"}]
    aliased = dim.select(
        F.col("turbine_id").alias("_dim_turbine_id"),
        *[F.col(c).alias(f"dim_{c}") for c in dim_cols],
    )
    condition = (readings["turbine_id"] == aliased["_dim_turbine_id"]) & (
        readings["event_ts"] >= aliased["dim___START_AT"]
    ) & (
        aliased["dim___END_AT"].isNull()
        | (readings["event_ts"] < aliased["dim___END_AT"])
    )
    return readings.join(aliased, condition, "left").drop("_dim_turbine_id")
