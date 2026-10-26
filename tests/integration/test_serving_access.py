"""Access test for the serving database: what the analyst and engineer roles can and cannot read.

Runs against the live serving database after a publish (``make access-test``); skipped elsewhere.
Each test logs in as the role itself, so these are the privileges a real user of that role gets.
See docs/access_control.md for the access matrix this test proves.
"""

from __future__ import annotations

import os
import re
from urllib.parse import urlsplit, urlunsplit

import psycopg2
import pytest
from psycopg2 import errors

SERVING_DSN = os.environ.get("SERVING_PG_DSN", "")
PASSWORDS = {"analyst": os.environ.get("ANALYST_PASSWORD", ""), "engineer": os.environ.get("ENGINEER_PASSWORD", "")}
PII_COLUMNS = ("full_name", "email", "phone", "date_of_birth")
MARTS = ("mart_daily_spend", "mart_advance_repayment", "mart_customer_features")
OTHER_DATABASES = ("source", "iceberg_catalog", "airflow")

pytestmark = pytest.mark.skipif(
    not (SERVING_DSN and all(PASSWORDS.values())),
    reason="needs SERVING_PG_DSN and role passwords (run inside the Airflow container: make access-test)",
)


def dsn_for(role: str, database: str = "serving") -> str:
    """The serving DSN with the role's own credentials and the given database."""
    parts = urlsplit(SERVING_DSN)
    netloc = f"{role}:{PASSWORDS[role]}@{parts.hostname}:{parts.port or 5432}"
    return urlunsplit((parts.scheme, netloc, f"/{database}", "", ""))


def query(role: str, statement: str) -> list[tuple]:
    with psycopg2.connect(dsn_for(role)) as conn, conn.cursor() as cur:
        cur.execute(statement)
        return cur.fetchall() if cur.description else []


def denied(role: str, statement: str) -> bool:
    """True if the statement fails with a privilege error for ``role``."""
    try:
        query(role, statement)
    except errors.InsufficientPrivilege:
        return True
    return False


# --- analyst ---------------------------------------------------------------------------------


@pytest.mark.parametrize("mart", MARTS)
def test_analyst_can_read_marts(mart: str) -> None:
    assert query("analyst", f"SELECT count(*) FROM serving.{mart}")[0][0] > 0


def test_analyst_can_read_masked_view_and_names_are_masked() -> None:
    rows = query(
        "analyst",
        "SELECT customer_id, name_masked, age_band, state, income_band FROM serving.v_customer_profile_masked",
    )
    assert rows
    assert all(re.fullmatch(r"\S\*\*\*", name) for _, name, *_ in rows)


@pytest.mark.parametrize("column", ["*", "customer_id", *PII_COLUMNS])
def test_analyst_is_denied_customer_profile(column: str) -> None:
    assert denied("analyst", f"SELECT {column} FROM serving.customer_profile LIMIT 1")


def test_analyst_has_no_privilege_on_any_personal_data_column() -> None:
    checks = ", ".join(
        f"has_column_privilege('analyst', 'serving.customer_profile', '{c}', 'SELECT')" for c in PII_COLUMNS
    )
    assert query("analyst", f"SELECT {checks}")[0] == (False,) * len(PII_COLUMNS)


def test_no_personal_data_column_in_anything_the_analyst_can_read() -> None:
    readable = query(
        "analyst",
        """
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        WHERE c.table_schema = 'serving'
          AND has_table_privilege('analyst', format('%I.%I', c.table_schema, c.table_name), 'SELECT')
        """,
    )
    tables = {t for t, _ in readable}
    assert tables == {*MARTS, "v_customer_profile_masked"}
    assert not {col for _, col in readable} & set(PII_COLUMNS)


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO serving.mart_daily_spend SELECT * FROM serving.mart_daily_spend LIMIT 1",
        "DELETE FROM serving.mart_customer_features",
        "CREATE TABLE serving.analyst_copy AS SELECT 1 AS x",
        "CREATE TABLE public.analyst_copy AS SELECT 1 AS x",
        "SET ROLE engineer",
    ],
)
def test_analyst_cannot_write_create_or_escalate(statement: str) -> None:
    assert denied("analyst", statement)


@pytest.mark.parametrize("database", OTHER_DATABASES)
@pytest.mark.parametrize("role", ["analyst", "engineer"])
def test_roles_cannot_connect_to_other_databases(role: str, database: str) -> None:
    with pytest.raises(psycopg2.OperationalError, match="permission denied for database"):
        psycopg2.connect(dsn_for(role, database)).close()


# --- engineer --------------------------------------------------------------------------------


def test_engineer_can_read_personal_data() -> None:
    rows = query("engineer", f"SELECT {', '.join(PII_COLUMNS)} FROM serving.customer_profile")
    assert rows and all(row[0] for row in rows)  # every profile has a full name
    masked = query("engineer", "SELECT count(*) FROM serving.v_customer_profile_masked")[0][0]
    assert masked == len(rows)


@pytest.mark.parametrize("mart", MARTS)
def test_engineer_can_read_marts(mart: str) -> None:
    assert query("engineer", f"SELECT count(*) FROM serving.{mart}")[0][0] > 0


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE serving.customer_profile SET full_name = 'x'",
        "TRUNCATE serving.mart_daily_spend",
        "CREATE TABLE serving.engineer_copy AS SELECT 1 AS x",
    ],
)
def test_engineer_is_read_only(statement: str) -> None:
    assert denied("engineer", statement)
