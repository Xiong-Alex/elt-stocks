"""
DAG contracts (lightweight TDD-style expectations).

How to use:
1) When you change workflow intent, update this expectation map first.
2) Then change DAG code until tests pass.

This keeps planned behavior explicit without requiring deep integration tests.
"""

from __future__ import annotations

from pathlib import Path

from tests.common.ast_contracts import bash_tasks_from_tree, dag_ids_from_tree, parse_file
from tests.common.paths import DAGS_DIR, python_files_under


# Explicit expectations by DAG file name.
# Keep this map small and behavioral: task ids + command fragments.
DAG_CONTRACTS = {
    "update_stock_universe_dag.py": {
        "dag_id": "update_stock_universe_dag",
        "tasks": {
            "refresh_stock_symbols": "refresh_stock_symbols.py",
            "build_dim_stock": "build_dim_stock.py",
            "build_dim_universe": "build_dim_universe.py",
            "build_bridge_stock_universe_membership": "build_bridge_stock_universe_membership.py",
        },
    },
    "historical_backfill_dag.py": {
        "dag_id": "historical_backfill_dag",
        "tasks": {
            "ingest_prices_to_kafka": "ingest_yfinance_to_kafka.py",
            "spark_to_bronze": "spark_kafka_to_bronze.py",
            "spark_to_silver": "spark_bronze_to_silver.py",
            "spark_to_gold": "spark_silver_to_gold.py",
            "build_fact_price_daily": "build_fact_price_daily.py",
        },
    },
    "intraday_pipeline_dag.py": {
        "dag_id": "intraday_pipeline_dag",
        "tasks": {
            "ingest_prices_to_kafka": "ingest_yfinance_to_kafka.py",
            "spark_to_bronze": "spark_kafka_to_bronze.py",
            "spark_to_silver": "spark_bronze_to_silver.py",
            "spark_to_gold": "spark_silver_to_gold.py",
        },
    },
    "price_bronze_streaming_dag.py": {
        "dag_id": "price_bronze_streaming_dag",
        "tasks": {
            "ingest_prices_to_kafka": "ingest_yfinance_to_kafka.py",
            "spark_to_bronze": "spark_kafka_to_bronze.py",
        },
    },
    "reference_data_dag.py": {
        "dag_id": "reference_data_dag",
        "tasks": {
            "ingest_fundamentals_raw": "ingest_yfinance_fundamentals.py",
            "ingest_dividends_raw": "ingest_yfinance_dividends.py",
            "ingest_earnings_raw": "ingest_yfinance_earnings.py",
            "build_fact_fundamentals": "build_fact_fundamentals.py",
            "build_fact_dividends": "build_fact_dividends.py",
            "build_fact_earnings": "build_fact_earnings.py",
        },
    },
    "feature_engineering_dag.py": {
        "dag_id": "feature_engineering_dag",
        "tasks": {
            "build_dim_date": "build_dim_date.py",
            "build_dim_stock": "build_dim_stock.py",
            "build_fact_price_daily": "build_fact_price_daily.py",
            "build_fact_fundamentals": "build_fact_fundamentals.py",
            "build_fact_dividends": "build_fact_dividends.py",
            "build_fact_earnings": "build_fact_earnings.py",
            "build_market_signals": "build_market_signals.py",
        },
    },
}


def test_dag_contract_map_is_not_empty() -> None:
    """
    Sanity check for test-data itself.

    If this fails, someone accidentally cleared the contract map and all
    "contract matching" tests become meaningless.
    """
    assert DAG_CONTRACTS, "DAG_CONTRACTS should not be empty"


def test_each_dag_contract_has_non_empty_task_map() -> None:
    """
    Ensure each DAG contract includes at least one expected task.

    This catches accidental placeholder contracts like:
    {"dag_id": "...", "tasks": {}}
    """
    for dag_file_name, contract in DAG_CONTRACTS.items():
        tasks = contract.get("tasks", {})
        assert tasks, f"{dag_file_name} should define at least one expected task in contract"


def test_all_contract_dag_files_exist() -> None:
    """Guardrail: every declared contract should map to an existing DAG file."""
    for dag_file_name in DAG_CONTRACTS:
        assert (DAGS_DIR / dag_file_name).exists(), f"Missing DAG file for contract: {dag_file_name}"


def test_contracts_cover_all_dag_files() -> None:
    """
    Coverage guardrail:
    - every DAG file in repo should have a declared contract entry.

    This is the "TDD-ish" safety net: when a new DAG is added, tests fail
    until a contract is added here.
    """
    discovered = {p.name for p in python_files_under(DAGS_DIR)}
    declared = set(DAG_CONTRACTS.keys())
    assert discovered == declared, (
        "DAG contract coverage mismatch.\n"
        f"Discovered only: {sorted(discovered - declared)}\n"
        f"Declared only: {sorted(declared - discovered)}"
    )


def test_dag_contracts_match() -> None:
    """
    Contract assertion:
    - expected dag_id exists
    - expected task ids exist
    - each expected task references expected command fragment
    """
    for dag_file_name, contract in DAG_CONTRACTS.items():
        # Read and parse DAG once per contract entry.
        dag_path = DAGS_DIR / dag_file_name
        tree = parse_file(dag_path)

        dag_ids = dag_ids_from_tree(tree)
        assert contract["dag_id"] in dag_ids, f"{dag_file_name} missing dag_id={contract['dag_id']}"

        tasks = bash_tasks_from_tree(tree)
        task_map = {t["task_id"]: t["bash_command"] for t in tasks}

        # Detect duplicate/empty extracted ids early.
        extracted_ids = [t["task_id"] for t in tasks]
        assert all(tid.strip() for tid in extracted_ids), f"{dag_file_name} has empty task_id in extracted tasks"
        assert len(extracted_ids) == len(set(extracted_ids)), f"{dag_file_name} has duplicate task_id values"

        for expected_task_id, expected_fragment in contract["tasks"].items():
            # Validate presence and command signature for each expected task.
            assert expected_task_id in task_map, f"{dag_file_name} missing task_id={expected_task_id}"
            assert expected_fragment in task_map[expected_task_id], (
                f"{dag_file_name}:{expected_task_id} missing command fragment {expected_fragment}"
            )

        # Keep contracts complete and explicit.
        # If you intentionally allow extra tasks, you can relax this in the future.
        assert set(task_map.keys()) == set(contract["tasks"].keys()), (
            f"{dag_file_name} task set mismatch.\n"
            f"Actual only: {sorted(set(task_map.keys()) - set(contract['tasks'].keys()))}\n"
            f"Contract only: {sorted(set(contract['tasks'].keys()) - set(task_map.keys()))}"
        )
