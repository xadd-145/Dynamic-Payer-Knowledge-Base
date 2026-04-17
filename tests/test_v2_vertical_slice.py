# tests/test_v2_vertical_slice.py
"""
Vertical slice tests for DPKB V2.
Tests the full workflow: DB → Resolver → Publisher → Notifications

Run: python tests/test_v2_vertical_slice.py

Expected: Ran 4 tests — OK
"""

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_init import get_connection, init_db, DB_PATH
from modules.module_e.resolver import resolve, resolve_all
from modules.module_c.rule_publisher import publish_fragment, reject_fragment


class TestVerticalSlice(unittest.TestCase):

    def setUp(self):
        """Get DB connection and load IDs before each test."""
        self.conn = get_connection()

        row = self.conn.execute(
            "SELECT rule_topic_id FROM rule_topics WHERE topic_code = 'ER'"
        ).fetchone()
        self.er_topic_id = row[0]

        row = self.conn.execute(
            "SELECT rule_topic_id FROM rule_topics WHERE topic_code = 'THER'"
        ).fetchone()
        self.ther_topic_id = row[0]

    def tearDown(self):
        self.conn.close()

    def test_1_resolver_returns_current_rule(self):
        """Staff queries ER rules on 2025-01-01 — should resolve to current version."""
        result = resolve(self.conn, self.er_topic_id, "2025-01-01", "date_of_service")
        self.assertEqual(result["resolution_status"], "resolved")
        self.assertIsNotNone(result["winning_rule_version_id"])
        self.assertIsNone(result["effective_end"])

    def test_2_resolver_returns_historical_rule(self):
        """Staff queries ER rules on 2022-09-15 — should return superseded v1."""
        result = resolve(self.conn, self.er_topic_id, "2022-09-15", "date_of_discharge")
        self.assertEqual(result["resolution_status"], "resolved")
        self.assertIsNotNone(result["winning_rule_version_id"])
        self.assertEqual(result["effective_end"], "2023-06-30")

    def test_3_resolve_all_returns_multiple_rules(self):
        """resolve_all on ER topic returns all active rules for that date."""
        result = resolve_all(self.conn, self.er_topic_id, "2025-01-01", "date_of_service")
        self.assertEqual(result["resolution_status"], "resolved")
        self.assertGreaterEqual(len(result["results"]), 1)
        for r in result["results"]:
            self.assertIn("rule_code", r)
            self.assertIn("rule_text", r)

    def test_4_publish_fragment_creates_rule_and_notification(self):
        """Admin approves TIER 2 fragment — rule_version and notification created."""
        # Get a pending TIER 2 fragment
        fragment = self.conn.execute(
            "SELECT policy_fragment_id FROM policy_fragments "
            "WHERE confidence_tier = 'TIER_2' AND review_status = 'pending' "
            "LIMIT 1"
        ).fetchone()

        if not fragment:
            self.skipTest("No TIER_2 pending fragment found — run seed_sample_data.py --reset")

        frag_id = fragment[0]
        notif_before = self.conn.execute(
            "SELECT COUNT(*) FROM notifications"
        ).fetchone()[0]

        result = publish_fragment(
            conn=self.conn,
            policy_fragment_id=frag_id,
            reviewed_by="admin_user",
            rule_code="THER-002",
            rule_name="PT Modifier Requirement Update",
            topic_code="THER",
            effective_start_date="2024-01-01",
            notes="Verified by vertical slice test",
        )

        self.assertEqual(result["status"], "published")
        self.assertIn("rule_version_id", result)

        # Verify rule_version exists and is published
        rv = self.conn.execute(
            "SELECT is_published, lifecycle_status FROM rule_versions WHERE rule_version_id = ?",
            (result["rule_version_id"],)
        ).fetchone()
        self.assertEqual(rv["is_published"], 1)
        self.assertEqual(rv["lifecycle_status"], "published")

        # Verify notification was created
        notif_after = self.conn.execute(
            "SELECT COUNT(*) FROM notifications"
        ).fetchone()[0]
        self.assertGreater(notif_after, notif_before)

        # Verify fragment marked approved
        frag = self.conn.execute(
            "SELECT review_status FROM policy_fragments WHERE policy_fragment_id = ?",
            (frag_id,)
        ).fetchone()
        self.assertEqual(frag["review_status"], "approved")


if __name__ == "__main__":
    unittest.main(verbosity=2)