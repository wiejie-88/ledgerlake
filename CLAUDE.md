# LedgerLake — Working Rules for Claude Code

## Project
LedgerLake is a 3-day portfolio data engineering project. The full specification is in `docs/SPEC.md`. Read it before planning any block.

## Workflow
- Work is split into blocks (D1-AM, D1-PM, … D3-EVE), listed in SPEC.md Section 8.
- Plan and implement ONE block at a time. Never start the next block without my approval.
- At the end of every block:
  1. Run the block's "Done when" verification and show me the actual output.
  2. Update the README if setup or usage changed.
  3. Suggest a git commit message. Do not commit unless I ask.

## Engineering rules
- Choose the simplest working option. No tools or services beyond SPEC.md without asking me first.
- Pin every version (Docker images, Python packages, JARs). Record them in `docs/decisions/001-versions.md`.
- Business logic goes in pure, testable functions under `jobs/` and `quality/`. Airflow DAG files only wire tasks together.
- Every job must be idempotent for a given logical date. Re-running must not create duplicates.
- Never commit secrets. Configuration comes from `.env`. Keep `.env.example` updated with placeholder values.
- Synthetic data only. Never use real personal data.
- If something stays blocked for more than about 30 minutes of attempts, stop. Explain the problem and propose the documented fallback (SPEC.md Section 8) instead of continuing to debug.
- Write one short ADR in `docs/decisions/` for each significant design decision: context, decision, alternatives considered, consequences.

## Honesty in documentation
- Report only measured numbers (row counts, run times, defects caught). Never estimate or invent metrics.
- If a feature is partial or skipped, say so in the README.

## Style
- Python 3.11, type hints, docstrings on public functions, ruff-clean.
- Clear log messages that include the table name, logical date and row counts.
