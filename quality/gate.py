"""Check results and the severity gate. No Spark, so it is unit-testable anywhere."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    ERROR = "error"  # a failure stops downstream tasks
    WARN = "warn"  # a failure is logged and the pipeline continues


@dataclass(frozen=True)
class CheckResult:
    table: str
    check_name: str
    severity: Severity
    passed: bool
    failed_rows: int
    details: str = ""


class DataQualityError(RuntimeError):
    """Raised when at least one error-severity check fails."""

    def __init__(self, failures: list[CheckResult]) -> None:
        self.failures = failures
        names = ", ".join(f"{r.table}:{r.check_name} ({r.failed_rows} rows)" for r in failures)
        super().__init__(f"{len(failures)} error-severity DQ check(s) failed: {names}")


def blocking_failures(results: list[CheckResult]) -> list[CheckResult]:
    """Failed checks with severity ``error``."""
    return [r for r in results if not r.passed and r.severity is Severity.ERROR]


def warnings(results: list[CheckResult]) -> list[CheckResult]:
    """Failed checks with severity ``warn``."""
    return [r for r in results if not r.passed and r.severity is Severity.WARN]


def enforce(results: list[CheckResult]) -> None:
    """Raise DataQualityError if any error-severity check failed; warnings never raise."""
    failures = blocking_failures(results)
    if failures:
        raise DataQualityError(failures)
