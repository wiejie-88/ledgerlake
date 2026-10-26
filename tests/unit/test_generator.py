"""Unit tests for the synthetic data generator (no database needed)."""

from __future__ import annotations

import random
from collections import Counter
from datetime import date, timedelta

import pytest

from generator import reference as ref
from generator import settings as cfg
from generator.run import seed_history, tick_day
from generator.synth import inject_transaction_defects, plan_base, plan_day
from tests.unit.memory_stores import MemoryEvents, MemorySource

LAST_HISTORY_DAY = cfg.HISTORY_START + timedelta(days=cfg.HISTORY_DAYS - 1)


@pytest.fixture(scope="module")
def seeded() -> tuple[MemorySource, MemoryEvents, dict[str, Counter[str]]]:
    """A full 60-day seed in memory (shared by the tests in this module; do not mutate)."""
    store, events = MemorySource(), MemoryEvents()
    totals = seed_history(store, events)
    return store, events, totals


def test_base_is_reproducible() -> None:
    assert plan_base().inserts == plan_base().inserts


def test_plan_day_is_reproducible(seeded) -> None:
    store, _, _ = seeded
    day = cfg.HISTORY_START + timedelta(days=10)
    state = store.load_state(day)
    first, second = plan_day(state, day), plan_day(state, day)
    assert first.inserts == second.inserts
    assert first.updates == second.updates
    assert first.defects == second.defects


def test_seed_is_reproducible(seeded) -> None:
    store, events, _ = seeded
    store2, events2 = MemorySource(), MemoryEvents()
    seed_history(store2, events2)
    assert store2.tables == store.tables
    assert events2.docs == events.docs


def test_seed_volumes_match_spec(seeded) -> None:
    store, events, _ = seeded
    assert len(store.tables["customers"]) == cfg.BASE_CUSTOMERS + cfg.NEW_CUSTOMERS_PER_DAY * cfg.HISTORY_DAYS == 2_000
    assert 95_000 <= len(store.tables["transactions"]) <= 105_000
    assert 45_000 <= len(events.docs) <= 55_000
    assert len(store.tables["cash_advances"]) > 0
    assert set(store.log) == {cfg.HISTORY_START - timedelta(days=1)} | {
        cfg.HISTORY_START + timedelta(days=i) for i in range(cfg.HISTORY_DAYS)
    }


def test_injected_defect_counts_match_the_data(seeded) -> None:
    store, events, totals = seeded
    txns = store.tables["transactions"]
    defects = totals["defects"]
    assert defects["transactions.duplicate_row"] == len(txns) - len({t["txn_id"] for t in txns})
    assert defects["transactions.null_amount"] == sum(t["amount"] is None for t in txns)
    assert defects["transactions.negative_amount"] == sum(t["amount"] is not None and t["amount"] < 0 for t in txns)
    assert defects["transactions.unknown_category"] == sum(t["category"] not in ref.CATEGORIES for t in txns)
    account_ids = {a["account_id"] for a in store.tables["accounts"]}
    assert defects["transactions.orphan_account"] == sum(t["account_id"] not in account_ids for t in txns)
    assert defects["app_events.duplicate_row"] == len(events.docs) - len({e["event_id"] for e in events.docs})
    assert defects["transactions.late_arriving_update"] > 0


def test_every_defect_type_is_present_at_two_to_three_percent(seeded) -> None:
    store, _, totals = seeded
    row_defects = [f"transactions.{name}" for name in cfg.TXN_DEFECT_RATES]
    assert all(totals["defects"][d] > 0 for d in row_defects)
    share = sum(totals["defects"][d] for d in row_defects) / len(store.tables["transactions"])
    assert 0.02 <= share <= 0.03


def test_late_arriving_updates_touch_older_rows(seeded) -> None:
    store, _, _ = seeded
    late = [t for t in store.tables["transactions"] if t["updated_at"].date() > t["txn_ts"].date()]
    assert late
    assert all(t["status"] in ref.TXN_STATUSES for t in late)


def test_schema_drift_starts_on_day_45(seeded) -> None:
    _, events, _ = seeded
    drift_start = cfg.HISTORY_START + timedelta(days=cfg.SCHEMA_DRIFT_DAY_INDEX)

    def local_day(e: dict) -> date:
        return (e["event_ts"] + cfg.MYT_OFFSET).date()

    assert all("referral_source" in e["properties"] for e in events.docs if local_day(e) >= drift_start)
    assert not any("referral_source" in e["properties"] for e in events.docs if local_day(e) < drift_start)


def test_reference_values_are_valid(seeded) -> None:
    store, events, _ = seeded
    assert {c["state"] for c in store.tables["customers"]} <= set(ref.STATES)
    assert {a["account_type"] for a in store.tables["accounts"]} <= set(ref.ACCOUNT_TYPES)
    assert {t["status"] for t in store.tables["transactions"]} <= set(ref.TXN_STATUSES)
    assert {t["direction"] for t in store.tables["transactions"]} == {"debit", "credit"}
    assert {a["status"] for a in store.tables["cash_advances"]} == set(ref.ADVANCE_STATUSES)
    assert {e["event_type"] for e in events.docs} == set(ref.EVENT_TYPES)
    assert all(c["email"].rsplit("@", 1)[1] in ref.EMAIL_DOMAINS for c in store.tables["customers"])


def test_salaries_arrive_on_paydays(seeded) -> None:
    store, _, _ = seeded
    paydays = {t["txn_ts"].day for t in store.tables["transactions"] if t["category"] == "salary"}
    assert paydays and paydays <= {25, 26, 27, 28}


def test_tick_adds_one_day_and_is_idempotent() -> None:
    store, events = MemorySource(), MemoryEvents()
    seed_history(store, events)
    next_day = LAST_HISTORY_DAY + timedelta(days=1)
    before = len(store.tables["transactions"])

    batch = tick_day(store, events, next_day)
    assert batch is not None
    assert batch.updates
    after_first = (len(store.tables["transactions"]), len(events.docs))
    assert after_first[0] > before

    assert tick_day(store, events, next_day) is None
    assert (len(store.tables["transactions"]), len(events.docs)) == after_first


def test_tick_requires_seed_and_valid_date() -> None:
    store, events = MemorySource(), MemoryEvents()
    with pytest.raises(RuntimeError, match="not seeded"):
        tick_day(store, events, cfg.HISTORY_START)
    store.reset()
    with pytest.raises(ValueError, match="before the history start"):
        tick_day(store, events, cfg.HISTORY_START - timedelta(days=1))


def test_inject_defects_keeps_clean_rows_unchanged() -> None:
    rows = [{"txn_id": f"T{i}", "account_id": "A0000001", "amount": 10, "category": "groceries"} for i in range(2_000)]
    out, counts = inject_transaction_defects(random.Random(1), [dict(r) for r in rows])
    assert len(out) == len(rows) + counts["duplicate_row"]
    untouched = [r for r in out if r["amount"] == 10 and r["category"] == "groceries" and r["account_id"] == "A0000001"]
    assert len(untouched) == len(rows) - sum(counts.values()) + 2 * counts["duplicate_row"]
