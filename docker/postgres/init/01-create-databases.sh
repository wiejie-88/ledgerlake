#!/usr/bin/env bash
# Creates the four LedgerLake databases. Postgres runs this only when its data
# volume is empty; run `make clean` to re-initialise. Safe to run twice.
set -euo pipefail

for db in source iceberg_catalog serving airflow; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
	SELECT format('CREATE DATABASE %I OWNER %I', '${db}', '${POSTGRES_USER}')
	WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db}')\gexec
EOSQL
  echo "init: database ${db} ready"
done
