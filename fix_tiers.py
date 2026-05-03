# fix_tiers.py
"""
One-time DB cleanup after SME governance fix.

Purpose:
  Move auto-escalated TIER_3 fragments back to Admin pending review.

Why:
  Earlier pipeline behavior wrote:
      TIER_3 -> review_status = 'escalated'

  New governance behavior is:
      TIER_1 -> pending Admin review
      TIER_2 -> pending Admin review
      TIER_3 -> pending Admin review

  SME should see only fragments manually escalated by Admin.

Safety:
  This script only changes TIER_3 fragments that:
    - currently have review_status = 'escalated'
    - have reviewed_by IS NULL

  That means it should not overwrite fragments already touched by a human.
"""

import sqlite3
from pathlib import Path


DB_PATH = Path("db") / "dpkb.db"


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))

    print("=" * 70)
    print("DPKB SME GOVERNANCE DB CLEANUP")
    print("=" * 70)

    print("\nBefore fix:")
    before = conn.execute("""
        SELECT confidence_tier, review_status, COUNT(*)
        FROM policy_fragments
        GROUP BY confidence_tier, review_status
        ORDER BY confidence_tier, review_status
    """).fetchall()

    for row in before:
        print(row)

    conn.execute("""
        UPDATE policy_fragments
        SET review_status = 'pending'
        WHERE confidence_tier = 'TIER_3'
          AND review_status = 'escalated'
          AND reviewed_by IS NULL
    """)

    changed = conn.total_changes
    conn.commit()

    print(f"\nRows updated: {changed}")

    print("\nAfter fix:")
    after = conn.execute("""
        SELECT confidence_tier, review_status, COUNT(*)
        FROM policy_fragments
        GROUP BY confidence_tier, review_status
        ORDER BY confidence_tier, review_status
    """).fetchall()

    for row in after:
        print(row)

    print("\nSME queue check:")
    sme_count = conn.execute("""
        SELECT COUNT(*)
        FROM policy_fragments
        WHERE review_status = 'escalated'
          AND confidence_tier IN ('TIER_2', 'TIER_3')
    """).fetchone()[0]

    print(f"Fragments currently visible to SME: {sme_count}")

    conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()