# ADR 003: Synthetic source generator design

**Status:** accepted (D1-PM, 2026-10-26)

## Context
SPEC §5.1 needs reproducible synthetic data with deliberate defects. It has a `seed` command (60 days of history) and a `tick --date` command (one more day plus updates to existing rows). Later blocks extract from these sources incrementally and must be idempotent, so the generator's own behaviour has to be predictable.

## Decision
- **`seed` is a sequence of ticks.** Seed first writes a base of 1,700 customers who signed up before the history window. It then plans and writes each of the 60 days in order, the same way `tick` does. Seed and tick therefore can't drift apart, and late-arriving updates (pending transactions settling 1–14 days later) come out naturally.
- **Reproducible randomness.** Every random draw comes from a `random.Random` seeded with a fixed string (`SEED:day:<date>`, `SEED:settle:<txn_id>`, and so on). Per-entity plans such as settlement day, repayment day and payday are derived from the entity's id, so they don't depend on the order in which things are processed.
- **Pure planning, thin IO.** `generator/synth.py` is pure: it takes a state and a date and returns a `DayBatch`. `generator/store.py` is the only module that touches Postgres or MongoDB. Unit tests run a full 60-day seed against in-memory stores.
- **Idempotent `tick`.** Each generated day writes a row to `source.generator_log` in the same Postgres transaction as its data. A tick for a day that is already logged does nothing. MongoDB events for a day are replaced, not appended, so a retry after a failed Postgres commit leaves no duplicates.
- **`seed` always rebuilds.** It drops and recreates the source tables and the `app_events` collection.
- **Source realism:**
  - Postgres timestamps are local Malaysia time with no time zone. Silver converts them to UTC.
  - `transactions` has no primary key or foreign key, so injected duplicate rows and orphan account references can exist.
  - Emails use the RFC 2606 reserved domains `example.com/net/org`.
- **No Faker.** Faker has no Malaysian names locale: `ms_MY` doesn't exist, and `en_MS` covers only addresses. Names are combined at random from curated Malay, Chinese and Indian Malaysian name lists in `generator/reference.py`.

## Alternatives considered
- **Generate the whole history in one pass, then bulk load.** This is faster but would duplicate the tick logic and its update behaviour.
- **Faker with a non-Malaysian locale.** Names wouldn't fit the Malaysian fintech scenario.
- **A primary key on `transactions.txn_id` with duplicates under new ids.** Silver dedupes on the business key (SPEC §5.3), so duplicates must share `txn_id`.

## Consequences
- Seed writes day by day: 60 Postgres transactions plus 60 MongoDB batches.
- Changing any value in `generator/settings.py` or `generator/reference.py` changes the generated data.
- `generator_log` records the injected defect counts per day. D2-AM can reconcile quarantine counts against it.
