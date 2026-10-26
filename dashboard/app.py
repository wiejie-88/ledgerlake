"""LedgerLake marts dashboard (one page, Streamlit).

Connects to the serving database as the **analyst** role, so it can only read the marts and the masked customer
view: the same least-privilege access an analyst has. Run by the `dashboard` Compose service on port 8501.
"""

from __future__ import annotations

import os
from datetime import date

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

SERIES_1 = "#2a78d6"  # reference palette, categorical slot 1 (light theme); one series per chart

st.set_page_config(page_title="LedgerLake marts", layout="wide")


@st.cache_data(ttl=60)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run a read-only query as the analyst role."""
    with psycopg2.connect(os.environ["SERVING_ANALYST_DSN"]) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def money(value: float) -> str:
    return f"MYR {value:,.0f}"


try:
    days = query(
        "SELECT min(txn_date) AS first, max(txn_date) AS last, max(as_of_date) AS as_of FROM serving.mart_daily_spend"
    )
except psycopg2.Error:
    # Before the first publish, neither the analyst role nor the serving tables exist.
    st.warning("The serving database isn't published yet. Run `make seed` and `make run date=2026-09-29` first.")
    st.stop()
if days.empty or pd.isna(days.loc[0, "last"]):
    st.warning("No published data yet. Run `make run date=2026-09-29` first.")
    st.stop()
first, last, as_of = (days.loc[0, c] for c in ("first", "last", "as_of"))

st.title("LedgerLake marts")
st.caption(f"Published as of {as_of}. Read as the `analyst` role: no personal data is reachable from this page.")

default_start = max(first, date.fromordinal(last.toordinal() - 29))
start, end = st.date_input("Business dates", value=(default_start, last), min_value=first, max_value=last)

spend = query(
    "SELECT txn_date, category, total_spend::float AS spend, txn_count FROM serving.mart_daily_spend "
    "WHERE txn_date BETWEEN %s AND %s",
    (start, end),
)
daily = spend.groupby("txn_date", as_index=False).agg(spend=("spend", "sum"), transactions=("txn_count", "sum"))
by_category = spend.groupby("category", as_index=False)["spend"].sum().sort_values("spend", ascending=False)
cohorts = query(
    "SELECT cohort_month, advance_count, repaid_count, overdue_count, due_count, repayment_rate, "
    "repayment_rate_due, avg_days_late FROM serving.mart_advance_repayment ORDER BY cohort_month"
)
features = query(
    "SELECT month, count(*) AS customers, round(avg(spend_total), 2)::float AS avg_spend, "
    "round(avg(income_regularity_3m)::numeric, 3)::float AS avg_income_regularity, sum(advance_count) AS advances "
    "FROM serving.mart_customer_features GROUP BY month ORDER BY month"
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Completed spend", money(daily["spend"].sum()), help="Completed debits in the selected dates")
k2.metric("Transactions", f"{int(daily['transactions'].sum()):,}")
k3.metric("Avg spend per day", money(daily["spend"].mean()) if len(daily) else "-")
due = cohorts.dropna(subset=["repayment_rate_due"])
k4.metric(
    "Repayment rate (oldest due cohort)",
    f"{due.iloc[0]['repayment_rate_due']:.1%}" if len(due) else "-",
    help="Share of due advances repaid, for the oldest cohort with advances already due",
)

left, right = st.columns([3, 2])
with left:
    st.subheader("Completed spend per day")
    line = (
        alt.Chart(daily)
        .mark_line(color=SERIES_1, strokeWidth=2, point=alt.OverlayMarkDef(size=40, color=SERIES_1))
        .encode(
            x=alt.X("txn_date:T", title=None),
            y=alt.Y("spend:Q", title="MYR"),
            tooltip=[
                alt.Tooltip("txn_date:T", title="Date"),
                alt.Tooltip("spend:Q", title="Spend (MYR)", format=",.2f"),
                alt.Tooltip("transactions:Q", title="Transactions", format=","),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(line, use_container_width=True)
with right:
    st.subheader("Spend by category")
    bars = (
        alt.Chart(by_category)
        .mark_bar(color=SERIES_1, cornerRadiusEnd=4)
        .encode(
            x=alt.X("spend:Q", title="MYR"),
            y=alt.Y("category:N", sort="-x", title=None, axis=alt.Axis(labelLimit=200)),
            tooltip=[
                alt.Tooltip("category:N", title="Category"),
                alt.Tooltip("spend:Q", title="Spend (MYR)", format=",.2f"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(bars, use_container_width=True)
with st.expander("Table: spend per day and category"):
    st.dataframe(spend.sort_values(["txn_date", "category"]), hide_index=True, use_container_width=True)

st.subheader("Cash-advance repayment by request cohort")
c1, c2 = st.columns([2, 3])
with c1:
    rates = (
        alt.Chart(due)
        .mark_bar(color=SERIES_1, cornerRadiusEnd=4)
        .encode(
            x=alt.X("yearmonth(cohort_month):O", title="Cohort"),
            y=alt.Y(
                "repayment_rate_due:Q",
                title="Repaid among due",
                axis=alt.Axis(format="%"),
                scale=alt.Scale(domain=[0, 1]),
            ),
            tooltip=[
                alt.Tooltip("yearmonth(cohort_month):O", title="Cohort"),
                alt.Tooltip("repayment_rate_due:Q", title="Repaid among due", format=".1%"),
                alt.Tooltip("due_count:Q", title="Due advances"),
            ],
        )
        .properties(height=240)
    )
    st.altair_chart(rates, use_container_width=True)
with c2:
    st.dataframe(cohorts, hide_index=True, use_container_width=True)

st.subheader("Customer features by month")
st.dataframe(features, hide_index=True, use_container_width=True)

st.subheader("Customers (masked view)")
st.caption("`serving.v_customer_profile_masked`: name initial only, no contact details or date of birth.")
st.dataframe(
    query("SELECT * FROM serving.v_customer_profile_masked ORDER BY customer_id LIMIT 10"),
    hide_index=True,
    use_container_width=True,
)
