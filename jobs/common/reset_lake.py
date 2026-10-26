"""Drop every table in the lake's bronze, silver, quarantine, gold and ops namespaces (PURGE deletes data files).

Usage: ``python -m jobs.common.reset_lake`` (or ``make reset-lake``, which asks first).
"""

from __future__ import annotations

import logging

from jobs.common.spark import CATALOG, get_spark

NAMESPACES = ("bronze", "silver", "quarantine", "gold", "ops")

log = logging.getLogger("reset_lake")


def main() -> None:
    """Drop all lake tables."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = get_spark("reset_lake")
    existing = {row[0] for row in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()}
    for namespace in NAMESPACES:
        if namespace not in existing:
            continue
        for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{namespace}").collect():
            spark.sql(f"DROP TABLE {CATALOG}.{namespace}.{row['tableName']} PURGE")
            log.info("dropped table=%s.%s", namespace, row["tableName"])
    spark.stop()


if __name__ == "__main__":
    main()
