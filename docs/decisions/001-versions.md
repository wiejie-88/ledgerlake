# ADR 001: Pinned versions

**Status:** accepted (D1-AM, 2026-10-26)

## Context
CLAUDE.md requires every version to be pinned. The platform must run natively on Apple Silicon (arm64) with no emulation. The pins were checked against Docker Hub, Maven Central and PyPI on 2026-10-26.

## Decision

| Component | Pin | arm64 |
|---|---|---|
| Airflow base image | `apache/airflow:3.3.2-python3.11` | yes |
| Java runtime | `eclipse-temurin:17.0.20_8-jre` (`/opt/java/openjdk` copied into the Airflow image) | yes |
| PySpark (bundles Spark 3.5.9, Scala 2.12, Hadoop 3.3.4) | `pyspark==3.5.9` | n/a (JVM) |
| Iceberg Spark runtime | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.2` | n/a (JVM) |
| Iceberg AWS bundle | `org.apache.iceberg:iceberg-aws-bundle:1.10.2` | n/a (JVM) |
| Postgres JDBC driver | `org.postgresql:postgresql:42.7.14` | n/a (JVM) |
| Postgres | `postgres:17.10` | yes |
| MongoDB | `mongo:8.0.32` | yes |
| MinIO server | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` (see ADR 002) | yes |
| MinIO client | `pgsty/mc:RELEASE.2026-09-16T00-00-00Z` | yes |
| Python constraints | `constraints-3.3.2/constraints-3.11.txt` (official Airflow) | n/a |
| pymongo (image) | `pymongo==4.18.1` (same as Airflow constraints) | wheel available |
| psycopg2-binary (image) | `psycopg2-binary==2.9.13` (same as Airflow constraints) | wheel available |
| pytest (dev) | `pytest==9.1.1` | n/a |
| ruff (dev) | `ruff==0.17.0` | wheel available |

Faker (listed in SPEC §4) is not used. See ADR 003.

JAR SHA-256 values are in `docker/airflow/Dockerfile`. The image build fails if a JAR doesn't match. Maven Central publishes no SHA-256 for the Postgres driver, so that hash was computed locally after confirming the published SHA-1 (`0e7c94ce…`).

**Compatibility checks built into the image:**
- `pip check` runs after installing with Airflow's constraints file.
- The build asserts that PySpark ships `scala-library-2.12.*`, which matches the Iceberg runtime's `_2.12` suffix.
- `java -version` runs at build time.
- The smoke test logs the Spark, Iceberg and Java versions at runtime.

**Why Airflow 3.3.2 and not 2.11.2:** Airflow 2 reached end of life in April 2026. 3.3.2 is the current stable release and was more than five weeks old when chosen.

**Why Iceberg 1.10.2 and not 1.12.0:** both publish a Spark 3.5 runtime and their JDBC catalogs behave the same. The 1.10 line has seen the most real-world use with Spark 3.5, while 1.12.0 was about four weeks old.

**Why the Temurin JRE copy rather than `apt install openjdk-17`:** copying from the Temurin image pins the exact JRE build, while the apt package would drift with Debian security updates.

## Alternatives considered
- Airflow 2.11.2: end of life.
- Downloading JARs at runtime with `spark.jars.packages` (Ivy): this needs network access on every cold start, and runtime-loaded JARs can't be found by the JDBC catalog's `DriverManager`.
- Separate Spark distribution: unnecessary, because the PyPI PySpark package contains the full runtime.

## Consequences
- Upgrading means changing the version args and SHA-256 values in the Dockerfile and updating this ADR.
- The constraints file pins `pandas==3.0.5`, which may not be compatible with PySpark 3.5's pandas integration. It isn't used yet; re-check this in the first block that converts between Spark and pandas.
