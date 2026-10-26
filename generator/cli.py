"""Command line: ``python -m generator seed`` and ``python -m generator tick --date YYYY-MM-DD``."""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, timedelta

from generator import settings as cfg
from generator.run import format_totals, seed_history, tick_day
from generator.store import MongoEvents, PostgresSource

log = logging.getLogger("generator")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="generator", description="LedgerLake synthetic source data")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("seed", help=f"rebuild the full {cfg.HISTORY_DAYS}-day history (wipes existing source data)")
    tick = sub.add_parser("tick", help="add one new day of data and update some existing rows")
    tick.add_argument("--date", required=True, type=date.fromisoformat, help="logical date, YYYY-MM-DD")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    store = PostgresSource(os.environ["SOURCE_PG_DSN"])
    events = MongoEvents(os.environ["MONGO_URI"])
    started = time.monotonic()
    try:
        if args.command == "seed":
            last = cfg.HISTORY_START + timedelta(days=cfg.HISTORY_DAYS - 1)
            log.info("seed: rebuilding source history %s..%s", cfg.HISTORY_START, last)
            totals = seed_history(store, events)
            for line in format_totals(totals):
                log.info("seed total: %s", line)
            measured = {**store.measure(), **events.measure()}
            for name, value in measured.items():
                log.info("seed measured in database: %s=%s", name, value)
        else:
            tick_day(store, events, args.date)
    finally:
        store.close()
        events.close()
    log.info("%s finished in %.1fs", args.command, time.monotonic() - started)
