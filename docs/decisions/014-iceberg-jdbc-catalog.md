# ADR 014: Iceberg JDBC catalog stored in Postgres

**Status:** accepted (D1-AM, recorded in D3-EVE, 2026-10-26). SPEC §3.3 key decision 2.

## Context
Iceberg needs a catalog that maps table names to their current metadata file and commits atomically. The options were Hadoop (filesystem), Hive Metastore, Nessie, a REST catalog, AWS Glue, or JDBC.

## Decision
Use Iceberg's **JDBC catalog**, configured in `jobs/common/spark.py`:
- the catalog is named `lake`, with `type=jdbc`;
- its tables live in the Postgres database `iceberg_catalog`;
- it uses `jdbc.schema-version=V1`.

Iceberg creates `iceberg_tables` and `iceberg_namespace_properties` on first use. Postgres is already in the stack for the source, serving and Airflow metadata, so the catalog adds no new service.

## Alternatives considered
- **Hadoop catalog on S3/MinIO:** no extra service, but commits rely on atomic rename, which S3 doesn't provide. It was kept as the documented fallback (SPEC §8) and wasn't needed: the JDBC wiring worked first time in D1-AM.
- **Hive Metastore or Nessie:** another JVM service and image to run and pin on a laptop.
- **REST catalog (for example Polaris or Lakekeeper):** the modern choice for multi-engine access, but another service. Worth it once a second engine (Trino, out of scope) needs the tables.
- **Glue:** needs AWS. It's the target on migration (ADR 013).

## Consequences
- The catalog is only as available as Postgres. `make clean` wipes it with the other volumes, so lake data and catalog stay consistent.
- Commits are atomic: a row-level compare-and-swap on `metadata_location` in Postgres.
- Any engine with Iceberg JDBC catalog support (Spark, Flink, pyiceberg's SQL catalog) can read the same tables.
