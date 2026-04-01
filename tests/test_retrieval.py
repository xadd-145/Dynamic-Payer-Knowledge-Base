# tests/test_retrieval.py
"""
Retrieval tests for the DPKB resolver.

Expected workflow:
1. Run db_init.py
2. Run seed_sample_data.py --reset
3. Run this test file

These tests validate the locked MVP behaviors:
- normal retrieval
- supersession historical retrieval
- supersession current retrieval
- no-match handling
- specificity tie-break
- resolution log creation
"""

from __future__ import annotations

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


import sqlite3

from db_init import DB_PATH, get_connection
from modules.module_e.resolver import resolve
from seed_sample_data import (
    ER_TOPIC_ID,
    TEST_TOPIC_ID,
    ER001_V1_ID,
    ER001_V2_ID,
    ER001_V3_ID,
    TIE_V1_ID,
)


def _assert_db_exists() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Run `python db_init.py --reset` and `python seed_sample_data.py --reset` first."
        )


def _reset_resolution_logs(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM resolution_logs")
    conn.commit()


def _count_resolution_logs(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM resolution_logs").fetchone()[0]


def test_1_normal_retrieval(conn: sqlite3.Connection) -> None:
    result = resolve(conn, ER_TOPIC_ID, "2025-01-01", "date_of_service")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == ER001_V3_ID, result
    assert result["effective_end"] is None, result


def test_2a_supersession_historical(conn: sqlite3.Connection) -> None:
    result = resolve(conn, ER_TOPIC_ID, "2024-03-15", "date_of_discharge")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == ER001_V1_ID, result
    assert result["effective_end"] == "2024-05-31", result


def test_2b_supersession_current(conn: sqlite3.Connection) -> None:
    result = resolve(conn, ER_TOPIC_ID, "2024-07-01", "date_of_discharge")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == ER001_V2_ID, result
    assert result["effective_end"] == "2024-12-31", result


def test_3_no_match(conn: sqlite3.Connection) -> None:
    result = resolve(conn, ER_TOPIC_ID, "2020-01-01", "date_of_service")

    assert result["resolution_status"] == "no_match", result
    assert result["winning_rule_version_id"] is None, result


def test_4_tiebreak_by_specificity(conn: sqlite3.Connection) -> None:
    result = resolve(conn, TEST_TOPIC_ID, "2024-01-01", "date_of_service")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == TIE_V1_ID, result


def test_5_resolution_logs_populated(conn: sqlite3.Connection) -> None:
    count = _count_resolution_logs(conn)
    assert count == 5, f"Expected 5 resolution logs, found {count}"


def run_all_tests() -> None:
    _assert_db_exists()

    conn = get_connection(DB_PATH)
    try:
        _reset_resolution_logs(conn)

        print("[tests] Running retrieval tests...\n")

        test_1_normal_retrieval(conn)
        print("PASS test_1_normal_retrieval")

        test_2a_supersession_historical(conn)
        print("PASS test_2a_supersession_historical")

        test_2b_supersession_current(conn)
        print("PASS test_2b_supersession_current")

        test_3_no_match(conn)
        print("PASS test_3_no_match")

        test_4_tiebreak_by_specificity(conn)
        print("PASS test_4_tiebreak_by_specificity")

        test_5_resolution_logs_populated(conn)
        print("PASS test_5_resolution_logs_populated")

        print("\n[tests] ALL TESTS PASSED")

    finally:
        conn.close()


if __name__ == "__main__":
    run_all_tests()