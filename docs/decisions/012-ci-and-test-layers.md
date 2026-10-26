# ADR 012: CI with GitHub Actions and layered tests

**Status:** accepted (D3-PM, 2026-10-26)

## Context
SPEC §2.2 and D3-PM ask for CI (GitHub Actions) that runs lint and unit tests on every push, a DAG import test, and pre-commit. Acceptance asks that every transformation function has unit tests.

Most transformations are PySpark, so they need Java and PySpark. The DAG test needs Airflow.

## Decision
- **Four test layers**, each collected only where its runtime exists (`tests/conftest.py` skips `spark/` without PySpark and `dags/` without Airflow; `integration/` skips without a live serving database):
  - `unit/`: pure Python (generator, extract helpers, DQ gate, serving contract).
  - `spark/`: every transformation (silver, gold, publish, demo audit, bronze metadata), DQ checks, reconciliation and MERGE change detection, all on in-memory DataFrames.
  - `dags/`: DAG integrity. It reads `DagBag.dags` from memory; `get_dag()` would need an initialised metadata database.
  - `integration/`: the serving access test against the running stack (`make access-test`).
- **The CI workflow** (`.github/workflows/ci.yml`) has two jobs on `ubuntu-24.04`:
  - `lint`: ruff check and ruff format check.
  - `test`: Temurin 17 via `actions/setup-java`, plus Python 3.11. It installs `apache-airflow==3.3.2` with the image's `requirements.txt` and `requirements-dev.txt` under the **same official Airflow constraints file** as the Docker image, then runs `pytest`.
- CI therefore checks the same pinned versions the image uses, without building the 4.4 GB image.
- All actions are pinned to release tags (`checkout@v7.0.1`, `setup-python@v7.0.0`, `setup-java@v6.0.1`). The workflow has read-only `contents` permission and cancels superseded runs.
- **Pre-commit** (`.pre-commit-config.yaml`): `ruff-check --fix` and `ruff-format` from `ruff-pre-commit` v0.17.0 (the same version as `requirements-dev.txt`), plus trailing whitespace, end-of-file, YAML, merge-conflict, large-file and private-key checks from `pre-commit-hooks` v6.0.0.
- **Local CI stand-in:** with no GitHub remote yet, the workflow's steps were run in a clean `python:3.11.16-slim-bookworm` container with `openjdk-17-jre-headless`, against a clean copy of the repository with no `.env`.

## Alternatives considered
- **Build the Docker image in CI and run `make test-container`:** the closest match to runtime, but slow (an image of about 4.4 GB) and redundant with pinned pip installs.
- **Run the integration access test in CI with service containers:** possible (Postgres and MinIO as services plus a seed and a pipeline run), but it would re-create the whole platform in CI. It stays a documented local command.
- **Mocking Spark:** loses the value of testing real DataFrame semantics such as NULL handling, decimals and time zones.

## Consequences
- CI time is dominated by the dependency install: 211 s for Airflow plus PySpark in the local stand-in. pip caching in Actions shortens repeat runs.
- "CI passes" on GitHub itself can only be confirmed once the repository is pushed to a GitHub remote.
