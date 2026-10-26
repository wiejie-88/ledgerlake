"""Iceberg smoke test: create, write and read a table on MinIO through the JDBC catalog.

Run inside the Airflow container: ``python -m jobs.common.smoke`` (or ``make smoke``).
Re-runnable: the table is dropped and recreated each time.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from jobs.common.spark import CATALOG, get_spark

TABLE = f"{CATALOG}.ops.smoke_test"
EXPECTED_IDS = [1, 2, 3]

log = logging.getLogger("smoke")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(f"SMOKE TEST FAILED: {message}")


def main() -> None:
    """Run the smoke test; raises RuntimeError on any failed check."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = get_spark("iceberg_smoke")
    jvm = spark.sparkContext._jvm
    log.info(
        "versions spark=%s iceberg=%s java=%s",
        spark.version,
        jvm.org.apache.iceberg.IcebergBuild.version(),
        jvm.java.lang.System.getProperty("java.version"),
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.ops")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.sql(f"CREATE TABLE {TABLE} (id INT, label STRING, created_at TIMESTAMP) USING iceberg")
    log.info("table=%s created", TABLE)

    now = datetime.now(UTC)
    rows = [(1, "alpha", now), (2, "beta", now), (3, "gamma", now)]
    spark.createDataFrame(rows, "id INT, label STRING, created_at TIMESTAMP").writeTo(TABLE).append()
    log.info("table=%s rows_written=%d", TABLE, len(rows))

    df = spark.table(TABLE)
    ids = sorted(r.id for r in df.select("id").collect())
    log.info("table=%s rows_read=%d ids=%s", TABLE, len(ids), ids)
    _require(ids == EXPECTED_IDS, f"expected ids {EXPECTED_IDS}, got {ids}")

    snapshots = spark.table(f"{TABLE}.snapshots").count()
    files = [r.file_path for r in spark.table(f"{TABLE}.files").select("file_path").collect()]
    location = spark.sql(f"DESCRIBE TABLE EXTENDED {TABLE}").where("col_name = 'Location'").first().data_type
    log.info("table=%s snapshots=%d data_files=%d location=%s", TABLE, snapshots, len(files), location)
    _require(snapshots == 1, f"expected 1 snapshot, got {snapshots}")
    _require(len(files) >= 1, "expected at least 1 data file")
    prefix = "s3://warehouse/ops/smoke_test/"
    _require(all(f.startswith(prefix) for f in files), f"data files not under {prefix}: {files}")

    spark.stop()
    log.info("SMOKE TEST PASSED")


if __name__ == "__main__":
    main()
