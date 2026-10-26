"""Seed and tick orchestration over storage-agnostic source and event stores."""

from __future__ import annotations

import logging
from collections import Counter
from datetime import date, timedelta
from typing import Any, Protocol

from generator import settings as cfg
from generator.model import EVENTS, DayBatch, Row, SourceState
from generator.synth import plan_base, plan_day

log = logging.getLogger("generator")


class SourceStore(Protocol):
    """The Postgres source database (or an in-memory stand-in for tests)."""

    def reset(self) -> None: ...
    def is_seeded(self) -> bool: ...
    def generated_days(self) -> set[date]: ...
    def load_state(self, day: date) -> SourceState: ...
    def apply(self, batch: DayBatch, kind: str) -> None: ...


class EventStore(Protocol):
    """The MongoDB app_events collection (or an in-memory stand-in for tests)."""

    def reset(self) -> None: ...
    def replace_day(self, day: date, docs: list[Row]) -> None: ...


def _apply(store: SourceStore, events: EventStore, batch: DayBatch, kind: str) -> None:
    # Events first: replace_day is idempotent, so if the Postgres commit then fails, a re-run is clean.
    if EVENTS in batch.inserts:
        events.replace_day(batch.day, batch.inserts[EVENTS])
    store.apply(batch, kind)
    inserted = " ".join(f"{t}={n}" for t, n in batch.inserted_counts().items())
    updated = " ".join(f"{t}={n}" for t, n in batch.updated_counts().items()) or "none"
    defects = " ".join(f"{d}={n}" for d, n in sorted(batch.defects.items()) if n) or "none"
    log.info(
        "logical_date=%s kind=%s inserted: %s | updated: %s | defects: %s", batch.day, kind, inserted, updated, defects
    )


def seed_history(store: SourceStore, events: EventStore) -> dict[str, Counter[str]]:
    """Rebuild the source from scratch: base customers, then every day of the history window.

    Returns totals: rows inserted and updated per table, and injected defects per type.
    """
    store.reset()
    events.reset()
    totals: dict[str, Counter[str]] = {"inserted": Counter(), "updated": Counter(), "defects": Counter()}

    _apply_and_total(store, events, plan_base(), "base", totals)
    for offset in range(cfg.HISTORY_DAYS):
        day = cfg.HISTORY_START + timedelta(days=offset)
        # Each day is planned from the state the previous days left behind.
        _apply_and_total(store, events, plan_day(store.load_state(day), day), "seed", totals)
    return totals


def _apply_and_total(
    store: SourceStore, events: EventStore, batch: DayBatch, kind: str, totals: dict[str, Counter[str]]
) -> None:
    _apply(store, events, batch, kind)
    totals["inserted"].update(batch.inserted_counts())
    totals["updated"].update(batch.updated_counts())
    totals["defects"].update(batch.defects)


def tick_day(store: SourceStore, events: EventStore, day: date) -> DayBatch | None:
    """Generate one new day. Returns None (and changes nothing) if ``day`` was already generated."""
    if not store.is_seeded():
        raise RuntimeError("Source is not seeded. Run `make seed` first.")
    if day < cfg.HISTORY_START:
        raise ValueError(f"tick date {day} is before the history start {cfg.HISTORY_START}")
    if day in store.generated_days():
        log.info("logical_date=%s already generated; nothing to do", day)
        return None
    batch = plan_day(store.load_state(day), day)
    _apply(store, events, batch, "tick")
    return batch


def format_totals(totals: dict[str, Any]) -> list[str]:
    """Render seed totals as log lines, one per table or defect type."""
    lines = [f"table={t} rows_inserted={n}" for t, n in sorted(totals["inserted"].items())]
    lines += [f"table={t} rows_updated={n}" for t, n in sorted(totals["updated"].items())]
    lines += [f"defect={d} injected={n}" for d, n in sorted(totals["defects"].items())]
    return lines
