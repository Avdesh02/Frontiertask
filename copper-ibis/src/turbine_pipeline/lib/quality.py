"""Data quality rules.

Rules are plain ``name -> SQL predicate`` strings so they serve three purposes at once:
they feed ``@dp.expect_all`` on the pipeline tables, they build the ``is_valid`` flag that
splits silver from quarantine, and they build the per-row ``violation_reasons`` array that
makes a quarantined row self-explanatory.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schemas import ACCEPTED_QUALITY_FLAGS

#: Rules every silver reading must satisfy. A row failing any of these is quarantined
#: rather than dropped, so nothing is lost silently.
READING_RULES: dict[str, str] = {
    "turbine_id_present": "turbine_id IS NOT NULL AND length(turbine_id) > 0",
    "tag_present": "tag IS NOT NULL AND length(tag) > 0",
    "event_ts_present": "event_ts IS NOT NULL",
    "value_present": "value IS NOT NULL",
    "value_is_finite": "value IS NULL OR NOT (isnan(value) OR abs(value) = double('inf'))",
    "quality_flag_accepted": (
        "quality IS NULL OR quality IN ("
        + ", ".join(f"'{flag}'" for flag in ACCEPTED_QUALITY_FLAGS)
        + ")"
    ),
    "event_ts_not_in_future": "event_ts <= current_timestamp() + interval 1 hour",
}

#: Rules applied to the turbine dimension CDC feed before it reaches AUTO CDC. AUTO CDC
#: needs a non-null key and a non-null sequence column or it cannot order changes.
TURBINE_CDC_RULES: dict[str, str] = {
    "turbine_id_present": "turbine_id IS NOT NULL AND length(turbine_id) > 0",
    "change_seq_present": "change_seq IS NOT NULL",
    "operation_recognised": "operation IN ('UPSERT', 'INSERT', 'UPDATE', 'DELETE')",
}


def all_rules_pass(rules: dict[str, str]) -> Column:
    """A single boolean Column that is true only when every rule holds."""
    condition = F.lit(True)
    for predicate in rules.values():
        condition = condition & F.expr(predicate)
    # A rule predicate can evaluate to NULL (e.g. a comparison against a NULL column).
    # Treat NULL as failure so such rows are quarantined rather than vanishing.
    return F.coalesce(condition, F.lit(False))


def violation_reasons(rules: dict[str, str]) -> Column:
    """An array of the names of every rule this row violates.

    Empty array for a clean row. Used to make quarantined rows diagnosable without
    re-running the rules by hand.
    """
    flagged = [
        F.when(~F.coalesce(F.expr(predicate), F.lit(False)), F.lit(name))
        for name, predicate in rules.items()
    ]
    return F.array_compact(F.array(*flagged))


def with_quality_flags(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Attach ``is_valid`` and ``violation_reasons`` to every row.

    Computing this once in a shared view -- rather than twice in the two downstream
    tables -- is what guarantees the silver and quarantine splits are exact complements.
    """
    return df.withColumn("is_valid", all_rules_pass(rules)).withColumn(
        "violation_reasons", violation_reasons(rules)
    )
