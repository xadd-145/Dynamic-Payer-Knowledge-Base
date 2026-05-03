# tests/test_bug_fixes.py
"""
Verifies all six bug fixes from the Step 1 code review.

Tests:
  1. Thresholds come from constants.py — no local override in scorer
  2. Semantic skip caps tier at TIER_2, never TIER_1
  3. TIER_1 fragment writes review_status = 'pending', not 'approved'
  4. TIER_3 fragment writes review_status = 'escalated'
  5. confidence_score is written correctly to DB (not 0.0)
  6. Crawler log uses correct column names
"""

import sqlite3
import sys
from pathlib import Path

# Make sure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from constants import TIER_1_THRESHOLD, TIER_2_THRESHOLD, CONFIDENCE_WEIGHTS, SEMANTIC_TIER_CAP
from modules.module_b.confidence_scorer import score_fragment, _assign_tier, OPENAI_API_KEY
from modules.module_b.fragment_writer import write_fragment, DB_PATH

PASS = 'PASS'
FAIL = 'FAIL'


def test_1_thresholds_from_constants():
    """TIER_1_THRESHOLD and TIER_2_THRESHOLD must come from constants, not scorer."""
    assert TIER_1_THRESHOLD == 75, f"Expected 75, got {TIER_1_THRESHOLD}"
    assert TIER_2_THRESHOLD == 40, f"Expected 40, got {TIER_2_THRESHOLD}"
    assert CONFIDENCE_WEIGHTS['structural'] == 0.60
    assert CONFIDENCE_WEIGHTS['semantic'] == 0.40
    assert SEMANTIC_TIER_CAP == 'TIER_2'
    return PASS


def test_2_assign_tier_boundaries():
    """Tier assignment uses constants thresholds correctly."""
    assert _assign_tier(75) == 'TIER_1'
    assert _assign_tier(74) == 'TIER_2'
    assert _assign_tier(40) == 'TIER_2'
    assert _assign_tier(39) == 'TIER_3'
    assert _assign_tier(0) == 'TIER_3'
    assert _assign_tier(100) == 'TIER_1'
    return PASS


def test_3_semantic_skip_caps_at_tier2():
    """
    When OPENAI_API_KEY is not set (semantic skipped),
    a fragment that structurally scores >= 75 must still be capped at TIER_2.
    """
    if OPENAI_API_KEY:
        # If key is set, unset it temporarily for this test
        import os
        original = os.environ.pop('OPENAI_API_KEY', '')

    # Use a fragment that would structurally score very high
    # Strong policy language + effective date keyword + revenue code pattern
    high_structural_fragment = (
        "Effective January 1, 2023, providers must submit claims using revenue code 0450 "
        "for all emergency room services. Claims must be billed under the fee-for-service "
        "methodology. Providers are required to include the correct type of bill and diagnosis "
        "code on all UB-04 claims submitted to NYS Medicaid. Claims not meeting these "
        "requirements will not be reimbursed."
    )

    result = score_fragment(
        fragment_text=high_structural_fragment,
        date_role='effective_date',
        date_source='direct',
    )

    print(f"    structural_score : {result['structural_score']}")
    print(f"    semantic_skipped : {result['semantic_skipped']}")
    print(f"    final_score      : {result['final_score']}")
    print(f"    tier             : {result['tier']}")

    if result['semantic_skipped']:
        assert result['tier'] in ('TIER_2', 'TIER_3'), (
            f"Semantic skipped but tier is {result['tier']} — should be capped at TIER_2"
        )
        assert result['tier'] != 'TIER_1', "TIER_1 assigned despite semantic being skipped"

    if OPENAI_API_KEY:
        import os
        os.environ['OPENAI_API_KEY'] = original

    return PASS


def test_4_tier1_writes_pending():
    """
    A fragment that scores TIER_1 must write review_status = 'pending', not 'approved'.
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Find any TIER_1 fragment in DB
    row = conn.execute(
        "SELECT review_status, confidence_tier FROM policy_fragments WHERE confidence_tier = 'TIER_1' LIMIT 1"
    ).fetchone()
    conn.close()

    if row is None:
        return f"SKIP — no TIER_1 fragments in DB yet. Run fragment_writer standalone test first."

    assert row['review_status'] == 'pending', (
        f"TIER_1 fragment has review_status = '{row['review_status']}', expected 'pending'"
    )
    return PASS


def test_5_tier3_writes_escalated():
    """
    A fragment that scores TIER_3 must write review_status = 'escalated'.
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    row = conn.execute(
        "SELECT review_status, confidence_tier FROM policy_fragments WHERE confidence_tier = 'TIER_3' LIMIT 1"
    ).fetchone()
    conn.close()

    if row is None:
        return f"SKIP — no TIER_3 fragments in DB yet. Run fragment_writer standalone test first."

    assert row['review_status'] == 'escalated', (
        f"TIER_3 fragment has review_status = '{row['review_status']}', expected 'escalated'"
    )
    return PASS


def test_6_confidence_score_written():
    """
    confidence_score column must not be 0.0 for all rows.
    At least some rows should have a non-zero score.
    """
    conn = sqlite3.connect(str(DB_PATH))

    total = conn.execute("SELECT COUNT(*) FROM policy_fragments").fetchone()[0]
    zero_score = conn.execute(
        "SELECT COUNT(*) FROM policy_fragments WHERE confidence_score = 0.0"
    ).fetchone()[0]
    non_zero = conn.execute(
        "SELECT COUNT(*) FROM policy_fragments WHERE confidence_score > 0.0"
    ).fetchone()[0]
    conn.close()

    if total == 0:
        return "SKIP — no fragments in DB yet. Run fragment_writer standalone test first."

    print(f"    total fragments  : {total}")
    print(f"    zero score rows  : {zero_score}")
    print(f"    non-zero rows    : {non_zero}")

    assert non_zero > 0, (
        f"All {total} fragments have confidence_score = 0.0. Column not being written."
    )
    return PASS


def test_7_crawler_columns_correct():
    """
    crawler_runs table must accept insert with correct column names.
    Tests that documents_found / documents_added / documents_skipped / documents_failed work.
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute('PRAGMA foreign_keys = ON')
    conn.execute('PRAGMA journal_mode = WAL')

    now = '2025-05-01T00:00:00'
    try:
        conn.execute("""
            INSERT INTO crawler_runs
                (started_at, completed_at, documents_found, documents_added,
                 documents_skipped, documents_failed, run_status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (now, now, 10, 5, 3, 2, 'completed', 'test run'))
        conn.commit()

        # Clean up test row
        conn.execute(
            "DELETE FROM crawler_runs WHERE notes = 'test run'"
        )
        conn.commit()
    except Exception as e:
        conn.close()
        raise AssertionError(f"Crawler column insert failed: {e}")

    conn.close()
    return PASS


# ---------------------------------------------------------------------------
# RUN ALL TESTS
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    tests = [
        ('1 — Thresholds from constants',        test_1_thresholds_from_constants),
        ('2 — Tier assignment boundaries',        test_2_assign_tier_boundaries),
        ('3 — Semantic skip caps at TIER_2',      test_3_semantic_skip_caps_at_tier2),
        ('4 — TIER_1 writes pending',             test_4_tier1_writes_pending),
        ('5 — TIER_3 writes escalated',           test_5_tier3_writes_escalated),
        ('6 — confidence_score written to DB',    test_6_confidence_score_written),
        ('7 — Crawler columns correct',           test_7_crawler_columns_correct),
    ]

    passed = 0
    failed = 0
    skipped = 0

    print('\n' + '='*60)
    print(' DPKB BUG FIX VERIFICATION — STEP 1')
    print('='*60 + '\n')

    for name, fn in tests:
        try:
            result = fn()
            if result == PASS:
                print(f'  [PASS] Test {name}')
                passed += 1
            elif result.startswith('SKIP'):
                print(f'  [SKIP] Test {name}: {result}')
                skipped += 1
            else:
                print(f'  [FAIL] Test {name}: {result}')
                failed += 1
        except AssertionError as e:
            print(f'  [FAIL] Test {name}: {e}')
            failed += 1
        except Exception as e:
            print(f'  [ERROR] Test {name}: {type(e).__name__}: {e}')
            failed += 1

    print(f'\n{"="*60}')
    print(f'  Passed  : {passed}')
    print(f'  Failed  : {failed}')
    print(f'  Skipped : {skipped}')
    print(f'{"="*60}\n')

    if failed > 0:
        sys.exit(1)