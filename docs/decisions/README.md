# Architecture decision records

Each ADR records the context, the decision, the alternatives considered, and the consequences.

| # | Decision | SPEC link |
|---|---|---|
| [001](001-versions.md) | Pinned versions (Airflow 3.3.2, PySpark 3.5.9, Iceberg 1.10.2, Temurin 17, Postgres 17, MongoDB 8) | §2.2 |
| [002](002-minio-community-image.md) | MinIO from the pgsty community image (the official images are no longer published) | §3.1 |
| [003](003-synthetic-generator.md) | Synthetic generator: seed as a sequence of ticks, reproducible, injected defects | §5.1 |
| [004](004-incremental-extract-watermarks.md) | Incremental extraction windows and watermarks (idempotent re-runs) | F2, F3 |
| [005](005-silver-quarantine-and-pii.md) | Silver: quarantine, late-update audit, PII hashing, schema evolution | §5.3 |
| [006](006-data-quality-module.md) | Lightweight DQ module and severity gate | Key decision 5, F6 |
| [007](007-gold-model-and-reconciliation.md) | Gold model, MERGE strategy, mart reconciliation | §5.4 |
| [008](008-airflow-orchestration.md) | Airflow `ledgerlake_daily`, business date, `ops.pipeline_runs` | F7 |
| [009](009-serving-and-access-control.md) | Serving via Postgres roles, masked view, transactional publish | Key decision 4, F8 |
| [010](010-time-travel-and-maintenance.md) | Time-travel audit demo and table maintenance | F9, should-have 7–8 |
| [011](011-write-efficiency.md) | 8 shuffle partitions, MERGE only changed rows (measured) | follow-up to 010 |
| [012](012-ci-and-test-layers.md) | GitHub Actions CI and test layers | §2.2 |
| [013](013-minio-and-aws-migration-path.md) | MinIO, and the migration path to S3 and Glue | Key decision 1, could-have 10 |
| [014](014-iceberg-jdbc-catalog.md) | Iceberg JDBC catalog in Postgres | Key decision 2 |
| [015](015-spark-local-mode-in-airflow.md) | Spark in local mode inside the Airflow container | Key decision 3 |
