"""Generator settings: seed, history window, volumes and defect rates.

Changing any value here changes the generated data; keep them fixed for reproducibility.
"""

from __future__ import annotations

from datetime import date, timedelta

SEED = 20260801

# History: 60 days starting HISTORY_START. Ticks continue from HISTORY_START + HISTORY_DAYS.
HISTORY_START = date(2026, 8, 1)
HISTORY_DAYS = 60

# Source timestamps are local Malaysia time (UTC+8), stored without a time zone in Postgres.
# MongoDB event_ts is stored as a UTC instant.
MYT_OFFSET = timedelta(hours=8)

# Customers: a base that signed up before the history window, plus daily sign-ups.
BASE_CUSTOMERS = 1_700
BASE_SIGNUP_SPAN_DAYS = 730
NEW_CUSTOMERS_PER_DAY = 5
SAVINGS_ACCOUNT_SHARE = 0.5

# Daily volumes (about 100k transactions and 50k events over 60 days)
TXNS_PER_DAY = 1_600
SAVINGS_TXN_SHARE = 0.1
EVENTS_PER_DAY = 830
ADVANCES_PER_DAY = (7, 13)
CUSTOMER_UPDATES_PER_DAY = 6
ACCOUNT_UPDATES_PER_DAY = 2

# Cash advances
ADVANCE_DUE_DAYS = 30
ADVANCE_MIN_TENURE_DAYS = 30

# Schema drift: app events from day 45 (index 44) onward carry properties.referral_source.
SCHEMA_DRIFT_DAY_INDEX = 44

# Injected defects. At most one defect per transaction row (about 2.2% in total).
TXN_DEFECT_RATES: dict[str, float] = {
    "duplicate_row": 0.006,
    "null_amount": 0.004,
    "negative_amount": 0.004,
    "unknown_category": 0.004,
    "orphan_account": 0.004,
}
EVENT_DUPLICATE_RATE = 0.005
