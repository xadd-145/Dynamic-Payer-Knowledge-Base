# tests/conftest.py
"""
Shared pytest fixtures for DPKB tests.
"""

import sys
import sqlite3
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_init import DB_PATH


@pytest.fixture
def conn():
    connection = sqlite3.connect(str(DB_PATH))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 5000")
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def ids(conn):
    output = {}

    # Topic IDs — both upper and lower key variants
    for row in conn.execute("SELECT rule_topic_id, topic_code FROM rule_topics"):
        code = row["topic_code"]
        tid  = row["rule_topic_id"]
        output[f"{code}_topic_id"]         = tid
        output[f"{code.lower()}_topic_id"] = tid

    # Atomic rule IDs
    for row in conn.execute("SELECT atomic_rule_id, rule_code FROM atomic_rules"):
        output[f"{row['rule_code']}_atomic_rule_id"] = row["atomic_rule_id"]

    # Latest published rule version per rule_code
    for row in conn.execute("""
        SELECT ar.rule_code, rv.rule_version_id
        FROM rule_versions rv
        JOIN atomic_rules ar ON ar.atomic_rule_id = rv.atomic_rule_id
        WHERE rv.lifecycle_status = 'published'
        ORDER BY ar.rule_code, rv.version_number DESC
    """):
        output.setdefault(
            f"{row['rule_code']}_rule_version_id",
            row["rule_version_id"]
        )

    # ER-001 specific v1 and v2 IDs plus v1 end date
    er001_versions = conn.execute("""
        SELECT rv.rule_version_id, rv.version_number, rv.effective_end_date
        FROM rule_versions rv
        JOIN atomic_rules ar ON ar.atomic_rule_id = rv.atomic_rule_id
        WHERE ar.rule_code = 'ER-001'
        ORDER BY rv.version_number ASC
    """).fetchall()

    for row in er001_versions:
        if row["version_number"] == 1:
            output["er001_v1_id"]  = row["rule_version_id"]
            output["er001_v1_end"] = row["effective_end_date"]
        elif row["version_number"] == 2:
            output["er001_v2_id"]  = row["rule_version_id"]

    return output