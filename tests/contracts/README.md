# Contract Tests (Light TDD Style)

These tests are intentionally lightweight and explicit.

## Why this folder exists

- You can change behavior by first updating expectations in contract maps.
- Then implement code until tests pass.
- This gives a "TDD-ish" workflow without heavy mocking/infrastructure.

## Files

- `test_dag_contracts.py`:
  - expected DAG IDs
  - expected task IDs
  - expected command fragments per task
- `test_job_contracts.py`:
  - expected job entrypoint names
  - expected core artifact tokens (table names, topic usage)

## Typical workflow

1. Plan a change.
2. Update relevant contract map in this folder.
3. Run tests (once pytest is installed): `python -m pytest`.
4. Implement code until contract tests pass.

