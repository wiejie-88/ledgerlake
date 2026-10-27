# ADR 015: Spark in local mode inside the Airflow container

**Status:** accepted (D1-AM, recorded in D3-EVE, 2026-10-26). SPEC §3.3 key decision 3.

## Context
The pipeline needs Spark for Iceberg MERGE, schema evolution, time travel and maintenance procedures. A Spark cluster (a master and workers) would add containers, memory and network setup to a laptop limited to 16 GB.

## Decision
Every job runs as `python -m <module>` inside the `airflow` container, with PySpark in **local mode** (`local[*]`). There's one driver JVM per task, and the driver is also the executor.
- The Airflow image contains Java 17 (Temurin), PySpark 3.5.9 and the Iceberg and JDBC JARs. The JARs go on `spark.driver.extraClassPath` so the JDBC catalog's `DriverManager` sees the Postgres driver.
- Settings come from `build_spark_conf`: `spark.driver.memory=2g` (`SPARK_DRIVER_MEMORY`), `spark.sql.shuffle.partitions=8` (ADR 011), and the session time zone set to UTC.
- Airflow runs one DAG run at a time (`max_active_runs=1`), so at most one Spark driver runs at once.

## Alternatives considered
- **A standalone Spark cluster in Compose with `SparkSubmitOperator`:** closer to production, but more memory and moving parts for no benefit at about 100k rows a day.
- **Separate per-task containers (DockerOperator / KubernetesPodOperator):** better isolation, but needs Docker-in-Docker or Kubernetes. Kubernetes is out of scope.
- **Pure Python (pandas, pyiceberg):** lighter, but doesn't support Iceberg MERGE, procedures or schema evolution the way Spark does.

## Consequences
- Each task pays about 10–15 s of JVM and Spark start-up. A full DAG run took 170–250 s in verification, most of it start-up and MERGE planning (measured).
- The jobs have no local-mode dependency, so the same modules run on EMR or Glue (ADR 013) with a different `master` and catalog configuration.
- Memory needed: Airflow standalone plus a 2 GB driver, with Postgres, MongoDB and MinIO alongside. That fits in an 8 GB Docker allocation.
