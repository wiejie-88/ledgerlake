"""Pure, deterministic planning of synthetic source data.

Nothing here touches a database. ``plan_base`` builds the customers who signed up before the
history window; ``plan_day`` takes the current source state and a date and returns that day's
inserts, updates and injected-defect counts. All randomness comes from ``stable_rng``, which is
seeded from fixed strings, so the same inputs always give the same output.
"""

from __future__ import annotations

import copy
import itertools
import math
import random
import re
import uuid
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import TypeVar

from generator import reference as ref
from generator import settings as cfg
from generator.model import EVENTS, DayBatch, Row, SourceState, Update

T = TypeVar("T")


def stable_rng(*parts: object) -> random.Random:
    """Return a Random seeded from the global seed plus ``parts``; stable across runs and machines."""
    return random.Random(":".join(str(p) for p in (cfg.SEED, *parts)))


def day_index(day: date) -> int:
    """Zero-based position of ``day`` in the history window (day 1 of the history is index 0)."""
    return (day - cfg.HISTORY_START).days


def to_utc(local: datetime) -> datetime:
    """Convert a naive Malaysia-time timestamp to an aware UTC timestamp."""
    return (local - cfg.MYT_OFFSET).replace(tzinfo=UTC)


def customer_profile(customer_id: str) -> tuple[int, bool]:
    """Return (payday of month, is salaried) for a customer; fixed for the customer's lifetime."""
    rng = stable_rng("profile", customer_id)
    return rng.randint(25, 28), rng.random() < 0.85


def settlement_plan(txn_id: str) -> tuple[int, str]:
    """Return (days after txn_ts when a pending transaction settles, final status)."""
    rng = stable_rng("settle", txn_id)
    offset = rng.choices((1, 2, 3, -1), weights=(50, 25, 15, 10))[0]
    if offset == -1:  # very late settlement
        offset = rng.randint(5, 14)
    status = rng.choices(("completed", "failed", "reversed"), weights=(90, 6, 4))[0]
    return offset, status


def repayment_offset(advance_id: str) -> int | None:
    """Return days after the request when a cash advance is repaid, or None if it never is."""
    rng = stable_rng("repay", advance_id)
    kind = rng.choices(("on_time", "late", "never"), weights=(65, 20, 15))[0]
    if kind == "on_time":
        return rng.randint(3, cfg.ADVANCE_DUE_DAYS)
    if kind == "late":
        return rng.randint(cfg.ADVANCE_DUE_DAYS + 1, cfg.ADVANCE_DUE_DAYS + 25)
    return None


# --- Small helpers ----------------------------------------------------------------------------


def _end_of(day: date) -> datetime:
    return datetime.combine(day, time(23, 59, 59))


def _time_on(rng: random.Random, day: date) -> datetime:
    hour = rng.choices(range(24), weights=ref.HOUR_WEIGHTS)[0]
    return datetime.combine(day, time(hour, rng.randrange(60), rng.randrange(60)))


def _money(value: float) -> Decimal:
    return Decimal(f"{max(value, 1.0):.2f}")


def _weighted(rng: random.Random, values: Sequence[T], weights: Sequence[float]) -> T:
    return rng.choices(values, weights=weights)[0]


def malaysian_name(rng: random.Random) -> str:
    """Return a fictional Malay, Chinese or Indian Malaysian full name."""
    group = _weighted(rng, ref.ETHNIC_GROUPS, ref.ETHNIC_WEIGHTS)
    female = rng.random() < 0.5
    if group == "malay":
        given = rng.choice(ref.MALAY_FEMALE if female else ref.MALAY_MALE)
        return f"{given} {'binti' if female else 'bin'} {rng.choice(ref.MALAY_FATHER)}"
    if group == "chinese":
        return f"{rng.choice(ref.CHINESE_SURNAMES)} {rng.choice(ref.CHINESE_GIVEN)}"
    given = rng.choice(ref.INDIAN_FEMALE if female else ref.INDIAN_MALE)
    return f"{given} {'a/p' if female else 'a/l'} {rng.choice(ref.INDIAN_FATHER)}"


def _email(rng: random.Random, full_name: str) -> str:
    words = [w for w in re.findall(r"[a-z]+", full_name.lower()) if w not in {"bin", "binti", "a", "l", "p"}]
    return f"{words[0]}.{words[-1]}{rng.randint(1, 9999)}@{rng.choice(ref.EMAIL_DOMAINS)}"


def _phone(rng: random.Random) -> str:
    prefix = rng.choice(ref.MOBILE_PREFIXES)
    digits = "".join(str(rng.randrange(10)) for _ in range(8 if prefix == "11" else 7))
    return f"+60 {prefix}-{digits[:3]} {digits[3:]}"


def make_customer(rng: random.Random, seq: int, created_at: datetime) -> Row:
    """Build one customer row (a new sign-up at ``created_at``)."""
    full_name = malaysian_name(rng)
    return {
        "customer_id": f"C{seq:06d}",
        "full_name": full_name,
        "email": _email(rng, full_name),
        "phone": _phone(rng),
        "date_of_birth": created_at.date() - timedelta(days=rng.randint(18 * 365, 65 * 365)),
        "state": _weighted(rng, ref.STATES, ref.STATE_WEIGHTS),
        "income_band": _weighted(rng, ref.INCOME_BANDS, ref.INCOME_WEIGHTS),
        "created_at": created_at,
        "updated_at": created_at,
    }


def make_accounts(rng: random.Random, customer_id: str, first_seq: int, opened_at: datetime) -> list[Row]:
    """Open a spending account for a new customer, plus a savings account for about half of them."""
    rows = [_account(first_seq, customer_id, "spending", opened_at)]
    if rng.random() < cfg.SAVINGS_ACCOUNT_SHARE:
        savings_at = min(opened_at + timedelta(seconds=rng.randint(60, 3600)), _end_of(opened_at.date()))
        rows.append(_account(first_seq + 1, customer_id, "savings", savings_at))
    return rows


def _account(seq: int, customer_id: str, account_type: str, opened_at: datetime) -> Row:
    return {
        "account_id": f"A{seq:07d}",
        "customer_id": customer_id,
        "account_type": account_type,
        "status": "active",
        "opened_at": opened_at,
        "updated_at": opened_at,
    }


# --- Base population --------------------------------------------------------------------------


def plan_base(start: date = cfg.HISTORY_START) -> DayBatch:
    """Customers and accounts that signed up during the two years before the history window."""
    rng = stable_rng("base")
    customers: list[Row] = []
    accounts: list[Row] = []
    window_start = datetime.combine(start, time())
    for seq in range(1, cfg.BASE_CUSTOMERS + 1):
        created_at = window_start - timedelta(seconds=rng.randint(86_400, cfg.BASE_SIGNUP_SPAN_DAYS * 86_400))
        customer = make_customer(rng, seq, created_at)
        customers.append(customer)
        accounts += make_accounts(rng, customer["customer_id"], len(accounts) + 1, created_at)
    return DayBatch(day=start - timedelta(days=1), inserts={"customers": customers, "accounts": accounts})


# --- One day ----------------------------------------------------------------------------------


def plan_day(state: SourceState, day: date) -> DayBatch:
    """Plan one day of source activity: sign-ups, transactions, advances, events and updates."""
    rng = stable_rng("day", day.isoformat())
    batch = DayBatch(day=day)

    customers, accounts = _new_customers(rng, state, day)
    batch.inserts["customers"] = customers
    batch.inserts["accounts"] = accounts
    batch.updates += _customer_updates(rng, state, day)
    batch.updates += _account_updates(rng, state, day)

    transactions, txn_defects = inject_transaction_defects(rng, _transactions(rng, state, day))
    batch.inserts["transactions"] = transactions
    batch.defects.update({f"transactions.{name}": n for name, n in txn_defects.items()})

    settlements = _settlements(rng, state, day)
    batch.updates += settlements
    batch.defects["transactions.late_arriving_update"] = len(settlements)

    batch.updates += _advance_updates(rng, state, day)
    batch.inserts["cash_advances"] = _new_advances(rng, state, day)

    events, duplicate_events = _events(rng, state, day)
    batch.inserts[EVENTS] = events
    batch.defects[f"{EVENTS}.duplicate_row"] = duplicate_events
    return batch


def _new_customers(rng: random.Random, state: SourceState, day: date) -> tuple[list[Row], list[Row]]:
    customers: list[Row] = []
    accounts: list[Row] = []
    for i in range(cfg.NEW_CUSTOMERS_PER_DAY):
        customer = make_customer(rng, state.next_customer_seq + i, _time_on(rng, day))
        customers.append(customer)
        accounts += make_accounts(
            rng, customer["customer_id"], state.next_account_seq + len(accounts), customer["created_at"]
        )
    return customers, accounts


def _customer_updates(rng: random.Random, state: SourceState, day: date) -> list[Update]:
    updates = []
    for customer in rng.sample(state.customers, min(cfg.CUSTOMER_UPDATES_PER_DAY, len(state.customers))):
        if rng.random() < 0.6:  # income band moves one step
            i = ref.INCOME_BANDS.index(customer.income_band)
            j = i + 1 if i == 0 else i - 1 if i == len(ref.INCOME_BANDS) - 1 else i + rng.choice((-1, 1))
            values: dict[str, object] = {"income_band": ref.INCOME_BANDS[j]}
        else:  # moves to another state
            values = {"state": rng.choice([s for s in ref.STATES if s != customer.state])}
        updates.append(Update("customers", customer.customer_id, {**values, "updated_at": _time_on(rng, day)}))
    return updates


def _account_updates(rng: random.Random, state: SourceState, day: date) -> list[Update]:
    savings = [a for a in state.accounts if a.account_type == "savings" and a.status == "active"]
    return [
        Update("accounts", a.account_id, {"status": "dormant", "updated_at": _time_on(rng, day)})
        for a in rng.sample(savings, min(cfg.ACCOUNT_UPDATES_PER_DAY, len(savings)))
    ]


def _channel(rng: random.Random, category: str) -> str:
    if category in ("transfer", "salary"):
        return "transfer"
    if category in ref.BILL_CATEGORIES:
        return "fpx" if rng.random() < 0.7 else "card"
    return _weighted(rng, ("card", "duitnow_qr", "ewallet"), (50, 25, 25))


def _transactions(rng: random.Random, state: SourceState, day: date) -> list[Row]:
    """Clean transactions for the day: salaries on paydays plus everyday spending and transfers."""
    active = [a for a in state.accounts if a.status == "active"]
    spending = [a for a in active if a.account_type == "spending"]
    savings = [a for a in active if a.account_type == "savings"]
    seq = itertools.count(1)
    rows: list[Row] = []

    def add(account_id: str, ts: datetime, amount: Decimal, direction: str, category: str, status: str) -> None:
        rows.append(
            {
                "txn_id": f"T{day:%Y%m%d}{next(seq):06d}",
                "account_id": account_id,
                "txn_ts": ts,
                "amount": amount,
                "direction": direction,
                "merchant_name": "PAYROLL" if category == "salary" else rng.choice(ref.MERCHANTS[category]),
                "category": category,
                "channel": _channel(rng, category),
                "status": status,
                "updated_at": min(ts + timedelta(seconds=rng.randint(0, 90)), _end_of(day)),
            }
        )

    spending_by_customer: dict[str, str] = {}
    for account in spending:
        spending_by_customer.setdefault(account.customer_id, account.account_id)
    for customer in state.customers:
        payday, salaried = customer_profile(customer.customer_id)
        if salaried and payday == day.day and customer.customer_id in spending_by_customer:
            ts = datetime.combine(day, time(rng.randint(6, 9), rng.randrange(60), rng.randrange(60)))
            amount = _money(ref.SALARY_BY_BAND[customer.income_band] * rng.uniform(0.95, 1.05))
            add(spending_by_customer[customer.customer_id], ts, amount, "credit", "salary", "completed")

    count = round(cfg.TXNS_PER_DAY * ref.DOW_FACTOR[day.weekday()] * rng.uniform(0.95, 1.05))
    for _ in range(count):
        if savings and rng.random() < cfg.SAVINGS_TXN_SHARE:
            account, category = rng.choice(savings), "transfer"
        else:
            account, category = rng.choice(spending), _weighted(rng, ref.SPEND_CATEGORIES, ref.SPEND_WEIGHTS)
        direction = rng.choice(("debit", "credit")) if category == "transfer" else "debit"
        median, sigma = ref.AMOUNT_PARAMS[category]
        amount = _money(median * math.exp(rng.gauss(0, sigma)))
        status = _weighted(rng, ref.TXN_STATUSES, ref.INITIAL_STATUS_WEIGHTS)
        add(account.account_id, _time_on(rng, day), amount, direction, category, status)
    return rows


def inject_transaction_defects(rng: random.Random, rows: list[Row]) -> tuple[list[Row], Counter[str]]:
    """Inject at most one defect per row at the rates in ``settings.TXN_DEFECT_RATES``.

    Returns the rows (duplicates appended next to their original) and a count per defect type.
    """
    out: list[Row] = []
    counts: Counter[str] = Counter()
    for row in rows:
        roll, threshold, defect = rng.random(), 0.0, None
        for name, rate in cfg.TXN_DEFECT_RATES.items():
            threshold += rate
            if roll < threshold:
                defect = name
                break
        if defect is None:
            out.append(row)
            continue
        counts[defect] += 1
        if defect == "duplicate_row":
            out += [row, dict(row)]
        elif defect == "null_amount":
            out.append({**row, "amount": None})
        elif defect == "negative_amount":
            out.append({**row, "amount": -row["amount"]})
        elif defect == "unknown_category":
            out.append({**row, "category": rng.choice(ref.UNKNOWN_CATEGORIES)})
        elif defect == "orphan_account":
            # Real account ids stay far below A9000000, so this never matches an account.
            out.append({**row, "account_id": f"A9{rng.randint(0, 999_999):06d}"})
    return out, counts


def _settlements(rng: random.Random, state: SourceState, day: date) -> list[Update]:
    """Settle pending transactions from earlier days: late-arriving updates to old rows."""
    updates = []
    for pending in state.pending_txns:
        offset, status = settlement_plan(pending.txn_id)
        if pending.txn_ts.date() + timedelta(days=offset) <= day:
            updates.append(Update("transactions", pending.txn_id, {"status": status, "updated_at": _time_on(rng, day)}))
    return updates


def _advance_updates(rng: random.Random, state: SourceState, day: date) -> list[Update]:
    updates = []
    for advance in state.open_advances:
        offset = repayment_offset(advance.advance_id)
        if offset is not None and advance.requested_at.date() + timedelta(days=offset) <= day:
            ts = _time_on(rng, day)
            values = {"status": "repaid", "repaid_at": ts, "updated_at": ts}
            updates.append(Update("cash_advances", advance.advance_id, values))
        elif advance.status == "active" and day > advance.due_date:
            ts = datetime.combine(day, time(0, 5)) + timedelta(seconds=rng.randrange(600))
            updates.append(Update("cash_advances", advance.advance_id, {"status": "overdue", "updated_at": ts}))
    return updates


def _new_advances(rng: random.Random, state: SourceState, day: date) -> list[Row]:
    open_customers = {a.customer_id for a in state.open_advances}
    tenure_cutoff = datetime.combine(day - timedelta(days=cfg.ADVANCE_MIN_TENURE_DAYS), time())
    eligible = [
        c.customer_id for c in state.customers if c.created_at < tenure_cutoff and c.customer_id not in open_customers
    ]
    chosen = rng.sample(eligible, min(len(eligible), rng.randint(*cfg.ADVANCES_PER_DAY)))
    rows = []
    for i, customer_id in enumerate(chosen):
        ts = _time_on(rng, day)
        rows.append(
            {
                "advance_id": f"ADV{state.next_advance_seq + i:06d}",
                "customer_id": customer_id,
                "amount": Decimal(_weighted(rng, ref.ADVANCE_AMOUNTS, ref.ADVANCE_WEIGHTS)),
                "requested_at": ts,
                "due_date": day + timedelta(days=cfg.ADVANCE_DUE_DAYS),
                "repaid_at": None,
                "status": "active",
                "updated_at": ts,
            }
        )
    return rows


def _app_version(rng: random.Random, index: int) -> str:
    current_minor = 2 + max(index, 0) // 14  # a new minor release every two weeks
    lag = _weighted(rng, (0, 1, 2), (70, 25, 5))
    return f"4.{max(current_minor - lag, 0)}.{rng.randint(0, 3)}"


def _events(rng: random.Random, state: SourceState, day: date) -> tuple[list[Row], int]:
    """App events for the day. From SCHEMA_DRIFT_DAY_INDEX onward, properties gain referral_source."""
    index = day_index(day)
    drift = index >= cfg.SCHEMA_DRIFT_DAY_INDEX
    docs: list[Row] = []
    duplicates = 0
    for _ in range(round(cfg.EVENTS_PER_DAY * rng.uniform(0.9, 1.1))):
        event_type = _weighted(rng, ref.EVENT_TYPES, ref.EVENT_WEIGHTS)
        properties: dict[str, str] = {} if event_type == "login" else {"offer_id": rng.choice(ref.OFFERS)}
        if drift:
            properties["referral_source"] = _weighted(rng, ref.REFERRAL_SOURCES, ref.REFERRAL_WEIGHTS)
        doc = {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "customer_id": rng.choice(state.customers).customer_id,
            "event_type": event_type,
            "event_ts": to_utc(_time_on(rng, day)),
            "device": {
                "os": _weighted(rng, ref.DEVICE_OS, ref.DEVICE_OS_WEIGHTS),
                "app_version": _app_version(rng, index),
            },
            "properties": properties,
        }
        docs.append(doc)
        if rng.random() < cfg.EVENT_DUPLICATE_RATE:
            docs.append(copy.deepcopy(doc))
            duplicates += 1
    return docs, duplicates
