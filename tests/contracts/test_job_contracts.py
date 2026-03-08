"""
Job contracts for key executable scripts.

Goal:
- keep expected job entrypoint names explicit
- keep expected "core artifact" references explicit (table/script tokens)

This is intentionally lightweight and readable for demo/framework development.
"""

from __future__ import annotations

from pathlib import Path

from tests.common.ast_contracts import parse_file
from tests.common.job_ast_checks import defined_functions, has_main_guard
from tests.common.paths import JOBS_DIR, python_files_under


JOB_CONTRACTS = {
    # ingest
    "ingest/ingest_yfinance_to_kafka.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["KAFKA_TOPIC", "stock_price_events_source"],
    },
    "ingest/ingest_yfinance_fundamentals.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["fundamentals_raw"],
    },
    "ingest/ingest_yfinance_dividends.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["dividends_raw"],
    },
    "ingest/ingest_yfinance_earnings.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["earnings_raw"],
    },
    # bronze/silver/gold
    "bronze/spark_kafka_to_bronze.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["stock_bars_bronze", "stock_bars_quarantine"],
    },
    "silver/spark_bronze_to_silver.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["stock_bars_silver"],
    },
    "gold/spark_silver_to_gold.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["stock_bars_gold"],
    },
    # marts
    "marts/build_dim_stock.py": {"entry_fn_any": {"run_job"}, "must_contain": ["dim_stock"]},
    "marts/build_dim_universe.py": {"entry_fn_any": {"run_job"}, "must_contain": ["dim_universe"]},
    "marts/build_bridge_stock_universe_membership.py": {
        "entry_fn_any": {"run_job"},
        "must_contain": ["bridge_stock_universe_membership"],
    },
    "marts/build_dim_date.py": {"entry_fn_any": {"run_job"}, "must_contain": ["dim_date"]},
    "marts/build_fact_price_daily.py": {"entry_fn_any": {"run_job"}, "must_contain": ["fact_price_daily"]},
    "marts/build_fact_fundamentals.py": {"entry_fn_any": {"run_job"}, "must_contain": ["fact_fundamentals"]},
    "marts/build_fact_dividends.py": {"entry_fn_any": {"run_job"}, "must_contain": ["fact_dividends"]},
    "marts/build_fact_earnings.py": {"entry_fn_any": {"run_job"}, "must_contain": ["fact_earnings"]},
    "marts/build_market_signals.py": {"entry_fn_any": {"run_job"}, "must_contain": ["fact_market_signals"]},
    "marts/refresh_stock_symbols.py": {
        "entry_fn_any": {"refresh_stock_symbols", "main"},
        "must_contain": ["stock_symbols", "stock_universe_memberships_source"],
    },
}


def test_job_contract_map_is_not_empty() -> None:
    """
    Sanity check for test-data itself.

    If this fails, someone accidentally cleared the contract map and all
    "contract matching" assertions become ineffective.
    """
    assert JOB_CONTRACTS, "JOB_CONTRACTS should not be empty"


def test_each_job_contract_has_required_keys() -> None:
    """
    Contract-schema check.

    Every job contract must include:
    - `entry_fn_any`
    - `must_contain`
    """
    for rel, contract in JOB_CONTRACTS.items():
        assert "entry_fn_any" in contract, f"{rel} missing contract key: entry_fn_any"
        assert "must_contain" in contract, f"{rel} missing contract key: must_contain"
        assert contract["entry_fn_any"], f"{rel} entry_fn_any should not be empty"
        assert contract["must_contain"], f"{rel} must_contain should not be empty"


def test_all_contract_job_files_exist() -> None:
    """Guardrail: every declared contract should map to an existing job file."""
    for rel in JOB_CONTRACTS:
        assert (JOBS_DIR / rel).exists(), f"Missing job file for contract: {rel}"


def test_contracts_cover_all_runnable_job_files() -> None:
    """
    Coverage guardrail for runnable job scripts.

    Rule:
    - every file with a `__main__` guard under `pipelines/jobs` must be declared
      in JOB_CONTRACTS.
    - utility-only modules (like `_db.py`, `__init__.py`) are excluded naturally
      because they do not have a main guard.
    """
    discovered_runnable: set[str] = set()
    for path in python_files_under(JOBS_DIR):
        rel = path.relative_to(JOBS_DIR).as_posix()
        tree = parse_file(path)
        if has_main_guard(tree):
            discovered_runnable.add(rel)

    declared = set(JOB_CONTRACTS.keys())
    assert discovered_runnable == declared, (
        "Job contract coverage mismatch.\n"
        f"Runnable only: {sorted(discovered_runnable - declared)}\n"
        f"Declared only: {sorted(declared - discovered_runnable)}"
    )


def test_job_contracts_match() -> None:
    """
    Contract assertion:
    - file has `if __name__ == "__main__"` guard
    - file defines at least one expected entry function
    - source contains expected artifact tokens
    """
    for rel, contract in JOB_CONTRACTS.items():
        # Parse once and reuse for all checks to keep logic readable.
        job_path = JOBS_DIR / rel
        source = job_path.read_text(encoding="utf-8")
        tree = parse_file(job_path)

        assert has_main_guard(tree), f"{rel} missing __main__ guard"

        fn_names = defined_functions(tree)
        assert fn_names & contract["entry_fn_any"], (
            f"{rel} missing expected entry function; expected one of {sorted(contract['entry_fn_any'])}"
        )

        # Contract hygiene: do not allow empty token lists in mappings.
        assert contract["must_contain"], f"{rel} has empty must_contain contract"

        for token in contract["must_contain"]:
            # Token checks act as lightweight "artifact usage" contracts.
            assert token in source, f"{rel} missing expected token '{token}'"
