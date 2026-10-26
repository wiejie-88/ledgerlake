"""Spark test fixtures. tests/conftest.py skips this package where PySpark isn't installed."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """A small local SparkSession with no catalog configured (pure DataFrame tests only)."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("ledgerlake-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
