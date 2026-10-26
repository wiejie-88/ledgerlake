"""SparkSession factory wired to the `lake` Iceberg catalog (JDBC catalog on Postgres, data on MinIO)."""

from __future__ import annotations

import os
from collections.abc import Mapping

from pyspark.sql import SparkSession

CATALOG = "lake"

_REQUIRED_ENV = (
    "LAKE_CATALOG_URI",
    "LAKE_CATALOG_USER",
    "LAKE_CATALOG_PASSWORD",
    "LAKE_WAREHOUSE",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
)


def build_spark_conf(env: Mapping[str, str]) -> dict[str, str]:
    """Build the Spark config for local mode with the `lake` Iceberg catalog.

    Args:
        env: Environment variables (normally ``os.environ``).

    Returns:
        Spark config keys mapped to values.

    Raises:
        KeyError: If a required environment variable is missing.
    """
    missing = [name for name in _REQUIRED_ENV if not env.get(name)]
    if missing:
        raise KeyError(f"Missing required environment variables: {', '.join(missing)}")

    cat = f"spark.sql.catalog.{CATALOG}"
    return {
        # JARs must be on the JVM classpath (not spark.jars) so the JDBC catalog's
        # DriverManager can find the Postgres driver. Local mode: one JVM.
        "spark.driver.extraClassPath": f"{env.get('SPARK_JARS_DIR', '/opt/spark-jars')}/*",
        "spark.driver.memory": env.get("SPARK_DRIVER_MEMORY", "2g"),
        # Spark's default of 200 shuffle partitions wrote up to 200 tiny files per MERGE; 8 suits a laptop.
        "spark.sql.shuffle.partitions": env.get("SPARK_SHUFFLE_PARTITIONS", "8"),
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.type": "jdbc",
        f"{cat}.uri": env["LAKE_CATALOG_URI"],
        f"{cat}.jdbc.user": env["LAKE_CATALOG_USER"],
        f"{cat}.jdbc.password": env["LAKE_CATALOG_PASSWORD"],
        f"{cat}.jdbc.schema-version": "V1",
        f"{cat}.warehouse": env["LAKE_WAREHOUSE"],
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": env["S3_ENDPOINT"],
        f"{cat}.s3.path-style-access": "true",
        f"{cat}.s3.access-key-id": env["S3_ACCESS_KEY"],
        f"{cat}.s3.secret-access-key": env["S3_SECRET_KEY"],
        f"{cat}.client.region": env.get("AWS_REGION", "us-east-1"),
    }


def get_spark(app_name: str) -> SparkSession:
    """Create (or reuse) a local-mode SparkSession configured for the `lake` catalog.

    Args:
        app_name: Spark application name, shown in logs.

    Returns:
        The active SparkSession.
    """
    builder = SparkSession.builder.master("local[*]").appName(app_name)
    for key, value in build_spark_conf(os.environ).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
