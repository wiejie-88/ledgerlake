"""Check definitions and their PySpark evaluation.

A check counts failing rows in a table. ``evaluate`` receives a ``load`` function that maps a table name
(``silver.transactions``) to a DataFrame, so checks never touch the catalog directly and are easy to test.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from quality.gate import CheckResult, Severity

Loader = Callable[[str], DataFrame]

# Quarantine reasons that are an audit trail, not rejected data (see ADR 005).
AUDIT_REASONS = ("superseded_by_late_update",)


@dataclass(frozen=True)
class Check:
    """One data-quality rule on one table."""

    table: str
    kind: str  # not_null | unique | accepted_values | positive | referential | freshness | quarantine_rate
    column: str
    severity: Severity = Severity.ERROR
    values: tuple[str, ...] = ()
    ref_table: str | None = None  # referential: parent table; quarantine_rate: bronze table
    ref_column: str | None = None
    max_lag_days: int = 0  # freshness
    threshold: float = 0.0  # quarantine_rate: maximum share of rejected rows

    @property
    def name(self) -> str:
        if self.kind == "referential":
            return f"referential:{self.column}->{self.ref_table}.{self.ref_column}"
        if self.kind == "freshness":
            return f"freshness:{self.column}<= {self.max_lag_days}d"
        if self.kind == "quarantine_rate":
            return f"quarantine_rate<={self.threshold:.0%}"
        return f"{self.kind}:{self.column}"


def count_nulls(df: DataFrame, column: str) -> int:
    """Rows where ``column`` is NULL."""
    return df.where(F.col(column).isNull()).count()


def count_duplicates(df: DataFrame, column: str) -> int:
    """Rows beyond the first for each non-null value of ``column``."""
    keyed = df.where(F.col(column).isNotNull())
    return keyed.count() - keyed.select(column).distinct().count()


def count_not_accepted(df: DataFrame, column: str, values: tuple[str, ...]) -> int:
    """Rows whose ``column`` is NULL or not in ``values``."""
    return df.where(~F.coalesce(F.col(column).isin(*values), F.lit(False))).count()


def count_non_positive(df: DataFrame, column: str) -> int:
    """Rows where ``column`` <= 0 (NULLs are the not_null check's job)."""
    return df.where(F.col(column) <= 0).count()


def count_orphans(df: DataFrame, column: str, parent: DataFrame, parent_column: str) -> int:
    """Rows whose non-null ``column`` has no match in ``parent[parent_column]``."""
    keys = parent.select(F.col(parent_column).alias("_k")).distinct()
    return df.where(F.col(column).isNotNull()).join(keys, F.col(column) == F.col("_k"), "left_anti").count()


def latest_value(df: DataFrame, column: str) -> date | None:
    """Latest date in ``column`` (a date or timestamp column), or None for an empty table."""
    value = df.agg(F.max(F.to_date(column))).first()[0]
    return value


def evaluate(check: Check, load: Loader, logical_date: date) -> CheckResult:
    """Run one check and return its result."""
    df = load(check.table)
    details = ""
    if check.kind == "not_null":
        failed = count_nulls(df, check.column)
    elif check.kind == "unique":
        failed = count_duplicates(df, check.column)
    elif check.kind == "accepted_values":
        failed = count_not_accepted(df, check.column, check.values)
    elif check.kind == "positive":
        failed = count_non_positive(df, check.column)
    elif check.kind == "referential":
        failed = count_orphans(df, check.column, load(check.ref_table), check.ref_column)
    elif check.kind == "freshness":
        latest = latest_value(df, check.column)
        oldest_allowed = logical_date - timedelta(days=check.max_lag_days)
        failed = 0 if latest is not None and latest >= oldest_allowed else 1
        details = f"latest={latest} required>={oldest_allowed}"
    elif check.kind == "quarantine_rate":
        failed, total = quarantine_counts(df, load(check.ref_table), logical_date)
        rate = failed / total if total else 0.0
        details = f"rejected={failed} bronze_rows={total} rate={rate:.4f}"
        return CheckResult(check.table, check.name, check.severity, rate <= check.threshold, failed, details)
    else:
        raise ValueError(f"unknown check kind {check.kind!r}")
    return CheckResult(check.table, check.name, check.severity, failed == 0, failed, details)


def quarantine_counts(quarantine: DataFrame, bronze: DataFrame, logical_date: date) -> tuple[int, int]:
    """(rejected rows quarantined for ``logical_date`` excluding audit rows, bronze rows for that date)."""
    day = F.lit(logical_date).cast("date")
    rejected = quarantine.where(
        (F.col("_quarantine_date") == day) & ~F.col("failure_reason").isin(*AUDIT_REASONS)
    ).count()
    return rejected, bronze.where(F.col("_logical_date") == day).count()
