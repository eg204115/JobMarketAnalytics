"""
Data Quality rule engine. Each rule is a small function returning a boolean
PySpark Column expression. Rules are tagged CRITICAL (row goes to quarantine
if failed) or WARNING (row is flagged but still promoted to Silver) — this
distinction matters: a missing description shouldn't block a row from being
analyzed, but a negative salary or an empty title should.

This is a genuine rule ENGINE, not a hardcoded filter chain, so adding a
new rule means adding one function + one registry entry, not restructuring
the whole transformation.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from utils.logger import get_logger

logger = get_logger(__name__)


class Severity(Enum):
    CRITICAL = "critical"   # failing rows are quarantined
    WARNING = "warning"     # failing rows are flagged but kept


@dataclass
class DQRule:
    name: str
    severity: Severity
    condition: Column   # a boolean column expression; TRUE = row PASSES this rule


def build_dq_rules(df: DataFrame) -> list[DQRule]:
    """
    Rules are built against a specific DataFrame's columns (not globally
    predefined) so they can reference derived columns created earlier in
    the pipeline (e.g., parsed_salary_min from the UDF step).
    """
    return [
        DQRule(
            name="title_not_empty",
            severity=Severity.CRITICAL,
            condition=F.col("title").isNotNull() & (F.trim(F.col("title")) != ""),
        ),
        DQRule(
            name="source_job_id_not_empty",
            severity=Severity.CRITICAL,
            condition=F.col("source_job_id").isNotNull() & (F.trim(F.col("source_job_id")) != ""),
        ),
        DQRule(
            name="salary_range_valid",
            severity=Severity.CRITICAL,
            # Passes if either salary is null (unknown salary is fine) OR
            # min <= max (a populated but inverted range is a real bad-data case we've seen).
            condition=(
                F.col("final_salary_min").isNull()
                | F.col("final_salary_max").isNull()
                | (F.col("final_salary_min") <= F.col("final_salary_max"))
            ),
        ),
        DQRule(
            name="company_present",
            severity=Severity.WARNING,
            condition=F.col("company").isNotNull() & (F.trim(F.col("company")) != ""),
        ),
        DQRule(
            name="description_present",
            severity=Severity.WARNING,
            condition=F.col("description").isNotNull() & (F.length(F.col("description")) > 20),
        ),
    ]


def apply_dq_rules(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Applies every rule, adds a `dq_warnings` array column (names of failed
    WARNING rules) to surviving rows, and splits the DataFrame into
    (clean_df, quarantined_df) based on CRITICAL rule failures.
    """
    rules = build_dq_rules(df)
    critical_rules = [r for r in rules if r.severity == Severity.CRITICAL]
    warning_rules = [r for r in rules if r.severity == Severity.WARNING]

    working_df = df

    # Track which CRITICAL rule(s) failed, for quarantine diagnostics.
    for rule in critical_rules:
        working_df = working_df.withColumn(f"_failed_{rule.name}", ~rule.condition)

    critical_fail_cols = [f"_failed_{r.name}" for r in critical_rules]
    working_df = working_df.withColumn(
        "_any_critical_failure",
        F.array_contains(F.array(*[F.col(c) for c in critical_fail_cols]), True),
    )

    # WARNING rules: build a list of failed rule names per row (kept regardless of pass/fail).
    warning_exprs = [
        F.when(~rule.condition, F.lit(rule.name)) for rule in warning_rules
    ]
    working_df = working_df.withColumn(
        "dq_warnings",
        F.array_except(F.array(*warning_exprs), F.array(F.lit(None).cast("string"))),
    )

    clean_df = working_df.filter(~F.col("_any_critical_failure")).drop(
        *critical_fail_cols, "_any_critical_failure"
    )
    quarantined_df = working_df.filter(F.col("_any_critical_failure")).drop(
        *critical_fail_cols, "_any_critical_failure"
    )

    logger.info(
        "DQ engine: %d rows passed, %d rows quarantined (critical rule failures)",
        clean_df.count(), quarantined_df.count(),
    )

    return clean_df, quarantined_df