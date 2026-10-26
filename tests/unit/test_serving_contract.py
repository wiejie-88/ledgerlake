"""The publish job's column lists must match the serving DDL (no Spark, no database)."""

from __future__ import annotations

import re
from pathlib import Path

from jobs.publish.contract import serving_columns

DDL = (Path(__file__).resolve().parents[2] / "serving" / "01_tables.sql").read_text()
PII_COLUMNS = {"full_name", "email", "phone", "date_of_birth"}


def ddl_columns() -> dict[str, tuple[str, ...]]:
    """Column names per table from the CREATE TABLE statements, in order."""
    tables = {}
    for name, body in re.findall(r"CREATE TABLE IF NOT EXISTS serving\.(\w+) \((.*?)\n\);", DDL, flags=re.S):
        columns = []
        for line in body.strip().splitlines():
            word = line.strip().split()[0]
            if word.upper() != "PRIMARY":
                columns.append(word)
        tables[name] = tuple(columns)
    return tables


def test_publish_columns_match_ddl() -> None:
    assert ddl_columns() == serving_columns()


def test_only_customer_profile_holds_personal_data() -> None:
    for table, columns in serving_columns().items():
        if table == "customer_profile":
            assert set(columns) >= PII_COLUMNS
        else:
            assert not PII_COLUMNS & set(columns), table


def test_masked_view_exposes_no_personal_data() -> None:
    view = (Path(__file__).resolve().parents[2] / "serving" / "02_views.sql").read_text()
    select = re.search(r"\nSELECT\n(.*?)\nFROM", view, flags=re.S).group(1)
    exposed = {line.strip().rstrip(",").split()[-1] for line in select.splitlines() if line.strip()}
    assert exposed == {"customer_id", "name_masked", "age_band", "state", "income_band"}
