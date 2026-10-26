# ADR 013: MinIO as the object store, and the AWS migration path

**Status:** accepted (D3-EVE, 2026-10-26). SPEC §3.3 key decision 1.

## Context
The project must run on a laptop at no cost, with no cloud account (SPEC §2.2). A real deployment would use AWS S3.

## Decision
- Use **MinIO** (the pgsty community image; see ADR 002) for the `warehouse` bucket.
- Talk to it only through the **S3 API**: Iceberg's `S3FileIO` with `path-style-access=true` and an explicit endpoint, and boto3 for the maintenance listing. No MinIO-specific API is used anywhere.

## Migration path to AWS
These are configuration changes, not code changes:

| Concern | Local (now) | AWS |
|---|---|---|
| Object store | MinIO, `s3://warehouse` | S3 bucket, `s3://<bucket>` |
| `spark.sql.catalog.lake.s3.endpoint` | `http://minio:9000` | unset (AWS default endpoint) |
| `s3.path-style-access` | `true` | unset (virtual-hosted style) |
| Credentials | `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `.env` | IAM role (instance profile, IRSA or EMR role); remove the key settings |
| Catalog | Iceberg JDBC catalog in Postgres (ADR 014) | AWS Glue Data Catalog: `catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`, `warehouse=s3://<bucket>`; the same `iceberg-aws-bundle` JAR contains it |
| Spark | Local mode in the Airflow container (ADR 015) | EMR or Glue jobs running the same `jobs.*` modules |
| Orchestration | Airflow standalone | MWAA or self-managed Airflow, same DAG; BashOperators become EMR or Glue operators |
| Serving | Postgres container | RDS PostgreSQL; `serving/*.sql` applies unchanged |

**Moving existing tables:** catalog migration is metadata-only. Register each table's current `metadata.json` in Glue (`CALL lake.system.register_table`), after copying the bucket with `aws s3 sync` and rewriting absolute paths if the bucket name changes.

## Consequences
- `jobs/common/spark.py` is the only place that knows about MinIO. `build_spark_conf` reads every endpoint and credential from the environment.
- No AWS path has been executed. The table above is a design, not a tested migration.
