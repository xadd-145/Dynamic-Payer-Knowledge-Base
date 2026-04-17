# tests/test_retrieval.py
"""
Retrieval tests for the DPKB resolver.

Run order:
    1. python db_init.py
    2. python seed_sample_data.py --reset
    3. python tests/test_retrieval.py

Seeded data this file tests against:
    ER-001 v1: 2022-01-01 to 2023-06-30  (specificity=8)
    ER-001 v2: 2023-07-01 to NULL         (specificity=8, current)
    ER-002 v1: 2022-01-01 to NULL         (specificity=7)
    INPT-001, THER-001, LAB-001: single versions each
"""

from __future__ import annotations

import sys
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_init import DB_PATH, get_connection
from modules.module_e.resolver import resolve


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _assert_db_exists() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Run `python db_init.py` and `python seed_sample_data.py --reset` first."
        )


def _reset_resolution_logs(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM resolution_logs")
    conn.commit()


def _count_resolution_logs(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM resolution_logs").fetchone()[0]


def _get_ids(conn: sqlite3.Connection) -> dict:
    """
    Look up all IDs needed by tests directly from the DB.
    No dependency on seed_sample_data exports.
    """
    # ER topic ID
    er_topic = conn.execute(
        "SELECT rule_topic_id FROM rule_topics WHERE topic_code = 'ER'"
    ).fetchone()
    assert er_topic, "ER topic not found — run seed_sample_data.py --reset"

    # INPT topic ID (used as a separate-topic no-match check)
    inpt_topic = conn.execute(
        "SELECT rule_topic_id FROM rule_topics WHERE topic_code = 'INPT'"
    ).fetchone()
    assert inpt_topic, "INPT topic not found — run seed_sample_data.py --reset"

    # ER-001 versions ordered by version_number
    er001_versions = conn.execute(
        """
        SELECT rv.rule_version_id, rv.effective_start_date, rv.effective_end_date
        FROM   rule_versions rv
        JOIN   atomic_rules  ar ON ar.atomic_rule_id = rv.atomic_rule_id
        WHERE  ar.rule_code = 'ER-001'
        ORDER  BY rv.version_number
        """
    ).fetchall()
    assert len(er001_versions) >= 2, (
        f"Expected at least 2 versions for ER-001, found {len(er001_versions)}. "
        "Run seed_sample_data.py --reset"
    )

    # ER-002 v1 (for tie-break test)
    er002_v1 = conn.execute(
        """
        SELECT rv.rule_version_id
        FROM   rule_versions rv
        JOIN   atomic_rules  ar ON ar.atomic_rule_id = rv.atomic_rule_id
        WHERE  ar.rule_code = 'ER-002'
        ORDER  BY rv.version_number
        LIMIT  1
        """
    ).fetchone()
    assert er002_v1, "ER-002 not found — run seed_sample_data.py --reset"

    return {
        "er_topic_id":   er_topic[0],
        "inpt_topic_id": inpt_topic[0],
        "er001_v1_id":   er001_versions[0][0],
        "er001_v1_end":  er001_versions[0][2],   # '2023-06-30'
        "er001_v2_id":   er001_versions[1][0],
        "er001_v2_end":  er001_versions[1][2],   # None
        "er002_v1_id":   er002_v1[0],
    }


# ---------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------

def test_1_normal_retrieval(conn: sqlite3.Connection, ids: dict) -> None:
    """Query date after v2 effective date — should return ER-001 v2 (current)."""
    result = resolve(conn, ids["er_topic_id"], "2025-01-01", "date_of_service")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == ids["er001_v2_id"], result
    assert result["effective_end"] is None, result


def test_2a_supersession_historical(conn: sqlite3.Connection, ids: dict) -> None:
    """Query date within v1 window — should return ER-001 v1 (superseded)."""
    result = resolve(conn, ids["er_topic_id"], "2022-09-15", "date_of_discharge")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == ids["er001_v1_id"], result
    assert result["effective_end"] == ids["er001_v1_end"], result   # '2023-06-30'


def test_2b_supersession_current(conn: sqlite3.Connection, ids: dict) -> None:
    """Query date within v2 window — should return ER-001 v2."""
    result = resolve(conn, ids["er_topic_id"], "2023-09-01", "date_of_discharge")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == ids["er001_v2_id"], result
    assert result["effective_end"] is None, result


def test_3_no_match(conn: sqlite3.Connection, ids: dict) -> None:
    """Query date before any rule exists — should return no_match."""
    result = resolve(conn, ids["er_topic_id"], "2020-01-01", "date_of_service")

    assert result["resolution_status"] == "no_match", result
    assert result["winning_rule_version_id"] is None, result


def test_4_tiebreak_by_specificity(conn: sqlite3.Connection, ids: dict) -> None:
    """
    Query date where both ER-001 v1 (specificity=8) and ER-002 v1 (specificity=7)
    are active. ER-001 v1 must win.
    Active on 2023-01-01:
        ER-001 v1: 2022-01-01 to 2023-06-30  ✓
        ER-002 v1: 2022-01-01 to NULL         ✓
    """
    result = resolve(conn, ids["er_topic_id"], "2023-01-01", "date_of_service")

    assert result["resolution_status"] == "resolved", result
    assert result["winning_rule_version_id"] == ids["er001_v1_id"], (
        f"Expected ER-001 v1 (id={ids['er001_v1_id']}) to win tie-break "
        f"but got version_id={result['winning_rule_version_id']}"
    )


def test_5_resolution_logs_populated(conn: sqlite3.Connection) -> None:
    """After tests 1-4, resolution_logs should have exactly 5 entries."""
    count = _count_resolution_logs(conn)
    assert count == 5, f"Expected 5 resolution logs, found {count}"


# ---------------------------------------------------------------------------
# RUNNER
# ---------------------------------------------------------------------------

def run_all_tests() -> None:
    _assert_db_exists()

    conn = get_connection(DB_PATH)
    try:
        ids = _get_ids(conn)
        _reset_resolution_logs(conn)

        print("[tests] Running retrieval tests...\n")
        print(f"  IDs loaded from DB:")
        print(f"    er_topic_id  = {ids['er_topic_id']}")
        print(f"    er001_v1_id  = {ids['er001_v1_id']}  (end {ids['er001_v1_end']})")
        print(f"    er001_v2_id  = {ids['er001_v2_id']}  (end {ids['er001_v2_end']})")
        print(f"    er002_v1_id  = {ids['er002_v1_id']}")
        print()

        test_1_normal_retrieval(conn, ids)
        print("PASS  test_1_normal_retrieval")

        test_2a_supersession_historical(conn, ids)
        print("PASS  test_2a_supersession_historical")

        test_2b_supersession_current(conn, ids)
        print("PASS  test_2b_supersession_current")

        test_3_no_match(conn, ids)
        print("PASS  test_3_no_match")

        test_4_tiebreak_by_specificity(conn, ids)
        print("PASS  test_4_tiebreak_by_specificity")

        test_5_resolution_logs_populated(conn)
        print("PASS  test_5_resolution_logs_populated")

        print("\n[tests] ALL 6 TESTS PASSED")

    finally:
        conn.close()


if __name__ == "__main__":
    run_all_tests()