"""Unit tests for the DQ severity gate (no Spark)."""

from __future__ import annotations

import pytest

from quality.gate import CheckResult, DataQualityError, Severity, blocking_failures, enforce, warnings


def _result(name: str, severity: Severity, passed: bool, failed: int = 0) -> CheckResult:
    return CheckResult("silver.transactions", name, severity, passed, failed)


def test_all_passing_does_not_raise() -> None:
    enforce([_result("not_null:amount", Severity.ERROR, True), _result("freshness", Severity.WARN, True)])


def test_warn_failure_does_not_raise() -> None:
    results = [_result("not_null:amount", Severity.ERROR, True), _result("freshness", Severity.WARN, False, 1)]
    enforce(results)
    assert [r.check_name for r in warnings(results)] == ["freshness"]
    assert blocking_failures(results) == []


def test_error_failure_raises_with_names() -> None:
    results = [
        _result("not_null:amount", Severity.ERROR, False, 3),
        _result("unique:txn_id", Severity.ERROR, True),
        _result("freshness", Severity.WARN, False, 1),
    ]
    with pytest.raises(DataQualityError, match=r"1 error-severity DQ check\(s\) failed: .*not_null:amount \(3 rows\)"):
        enforce(results)
    assert [r.check_name for r in blocking_failures(results)] == ["not_null:amount"]
