# LedgerLake Airflow image: Airflow 3 + Java 17 + PySpark 3.5 + Iceberg JARs.
# Every version is pinned; see docs/decisions/001-versions.md.

# Pinned JRE. Multi-arch, so arm64 hosts get the arm64 build.
FROM eclipse-temurin:17.0.20_8-jre AS jre

FROM apache/airflow:3.3.2-python3.11

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11
ARG ICEBERG_VERSION=1.10.2
ARG POSTGRES_JDBC_VERSION=42.7.14
ARG MAVEN=https://repo1.maven.org/maven2

USER root

COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH

# Spark JARs: downloaded from Maven Central and verified by SHA-256.
# They are loaded via spark.driver.extraClassPath (see jobs/common/spark.py).
ENV SPARK_JARS_DIR=/opt/spark-jars
RUN mkdir -p "$SPARK_JARS_DIR" && cd "$SPARK_JARS_DIR" \
 && curl -fsSLO "$MAVEN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/$ICEBERG_VERSION/iceberg-spark-runtime-3.5_2.12-$ICEBERG_VERSION.jar" \
 && curl -fsSLO "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/$ICEBERG_VERSION/iceberg-aws-bundle-$ICEBERG_VERSION.jar" \
 && curl -fsSLO "$MAVEN/org/postgresql/postgresql/$POSTGRES_JDBC_VERSION/postgresql-$POSTGRES_JDBC_VERSION.jar" \
 && printf '%s\n' \
    "c562d482dcd16b666e808b755469fbc4b2604284b41622c9d87e2c02b7cc6c5f  iceberg-spark-runtime-3.5_2.12-$ICEBERG_VERSION.jar" \
    "a2395ab49bf87e971e5588db644194ac18e9c0e0b44a7ad5a422a97648a38415  iceberg-aws-bundle-$ICEBERG_VERSION.jar" \
    "73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c  postgresql-$POSTGRES_JDBC_VERSION.jar" \
    | sha256sum -c - \
 && chmod 644 ./*.jar

USER airflow

# Install with Airflow's official constraints, then fail the build on any conflict.
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
 && pip check \
 && java -version

# The Iceberg runtime is built for Scala 2.12; fail the build if PySpark ships anything else.
RUN python -c "import glob, os, sys, pyspark; \
jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), 'jars', 'scala-library-2.12.*.jar')); \
print('scala-library:', jars); sys.exit(0 if jars else 1)"

# Project code. Compose bind-mounts these directories for live editing.
COPY --chown=airflow:root generator /opt/ledgerlake/generator
COPY --chown=airflow:root jobs /opt/ledgerlake/jobs
COPY --chown=airflow:root quality /opt/ledgerlake/quality
COPY --chown=airflow:root serving /opt/ledgerlake/serving
COPY --chown=airflow:root dags /opt/ledgerlake/dags
ENV PYTHONPATH=/opt/ledgerlake
WORKDIR /opt/ledgerlake
