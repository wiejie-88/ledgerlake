# ADR 005: Silver processing, quarantine, PII handling and schema evolution

**Status:** accepted (D2-AM, 2026-10-26)

## Context
SPEC §5.3 asks silver to:
- deduplicate on the business key, keeping the latest `updated_at`;
- cast types and convert timestamps to UTC;
- hash email and phone with a salt;
- reduce date of birth to an age band;
- keep `full_name` only in `silver.customers_pii`;
- quarantine failed rows with a reason;
- add `referral_source` to `silver.app_events` through Iceberg schema evolution, without rewriting the table.

D2-AM is done when every injected defect type appears in quarantine with a reason. That includes late-arriving updates.

## Decision
- **Incremental per logical date.** For each entity, `jobs/silver/run.py` reads bronze partition D and then runs these steps:
  1. **Validate:** check accepted values, NULL-safe not-null and `amount > 0` rules, and parent keys against the current silver parent table.
  2. **Deduplicate:** keep the newest row per key. Exact re-deliveries are quarantined as `duplicate_row`.
  3. **Audit late updates (transactions):** this follows the decision made with the project owner.
     - When the newest version's `updated_at` falls on a later day than its `txn_ts`, the latest earlier version from older bronze partitions is written to quarantine as `superseded_by_late_update`.
     - The new version is applied to silver normally.
     - These are audit rows, not data-quality failures. D2-MID's DQ results will count them separately.
  4. **Conform:** see the PII and timestamp rules below.
  5. **Write:** overwrite quarantine partition `_quarantine_date = D`, then `MERGE INTO` silver. A matched row is updated only when the incoming `updated_at` is newer. Re-running a date therefore changes nothing.
- **Quarantine tables** `quarantine.<entity>` keep the raw bronze columns, so rows can be inspected and replayed. They add `failure_reason`, with rules separated by `; ` when a row fails several, plus `_quarantine_date` and `_quarantined_at`.
- **PII:**
  - `silver.customers` holds `email_hash` = SHA-256(`PII_SALT` + lower(trim(email))), `phone_hash` = SHA-256(`PII_SALT` + digits only), and `age_band` as of the logical date.
  - Name, email, phone and date of birth exist only in `silver.customers_pii`.
  - The salt comes from `.env`, and the job refuses to run without it.
- **Timestamps:** Postgres naive Malaysia-time timestamps become UTC (`to_utc_timestamp(..., 'Asia/Kuala_Lumpur')`). `silver.transactions` also keeps `txn_date`, the Malaysia business date. `silver.app_events` keeps `event_date` the same way.
- **Schema evolution:** `silver.app_events` starts with a fixed base schema: the flattened device fields and `offer_id`. Events are loaded in event-time order. When a property key that isn't yet a column first appears, `ALTER TABLE ADD COLUMN` runs before the events carrying it are inserted. Earlier data files are never rewritten. Keys that aren't safe column names stay only in bronze's `properties` map.

## Alternatives considered
- **Rebuild silver from all of bronze on every run:** simpler, but it rewrites every table daily, which undermines the "no rewrite" schema-evolution requirement.
- **Apply late updates without auditing them:** cleaner quarantine semantics, but it doesn't meet the D2-AM "Done when". The project owner chose the audit approach.
- **Infer the events schema from data on first load:** the first, initial-load run would already contain `referral_source`, so no evolution would ever take place.

## Consequences
- An initial load spanning the drift date still demonstrates evolution. The pre-drift events are committed under the base schema first.
- Superseded versions are detected only when an earlier version reached bronze. That means late updates are audited from the second loaded day onward, not inside the initial full load.
- An invalid newer version, such as a settled transaction with a null amount, is quarantined again under its defect reason. So quarantine `rows` can exceed distinct keys.
- A no-op MERGE still commits an empty Iceberg snapshot.
