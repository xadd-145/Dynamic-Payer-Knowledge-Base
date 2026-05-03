# scripts/verify_step1_fixes.py
"""
Step 1 verification — confirms all six governance/safety fixes are in place.

Run from project root:
    python scripts/verify_step1_fixes.py

Expected output: ALL 6 CHECKS PASSED
"""

import sys
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')

from constants import TIER_1_THRESHOLD, TIER_2_THRESHOLD
from modules.module_b import confidence_scorer
from modules.module_b.confidence_scorer import score_fragment
from modules.module_b.fragment_writer import write_fragment, DB_PATH


def assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def assert_true(condition, label):
    if not condition:
        raise AssertionError(label)


# ---------------------------------------------------------------------------
# TEST 1 — Thresholds come from constants, not scorer
# ---------------------------------------------------------------------------

def test_1_thresholds_from_constants():
    print("Test 1 — Thresholds from constants.py ...")
    assert_eq(TIER_1_THRESHOLD, 75, "TIER_1_THRESHOLD in constants")
    assert_eq(TIER_2_THRESHOLD, 40, "TIER_2_THRESHOLD in constants")
    assert_eq(
        confidence_scorer.TIER_1_THRESHOLD, 75,
        "confidence_scorer.TIER_1_THRESHOLD must equal constants value"
    )
    assert_eq(
        confidence_scorer.TIER_2_THRESHOLD, 40,
        "confidence_scorer.TIER_2_THRESHOLD must equal constants value"
    )
    print("  PASS")


# ---------------------------------------------------------------------------
# TEST 2 — Semantic skipped caps tier at TIER_2
# ---------------------------------------------------------------------------

def test_2_semantic_skipped_cap():
    print("Test 2 — Semantic skipped caps at TIER_2 ...")

    original_key = confidence_scorer.OPENAI_API_KEY
    confidence_scorer.OPENAI_API_KEY = ""

    strong = (
        "Effective July 1, 2025, hospital Outpatient Departments and Diagnostic "
        "and Treatment Centers will be eligible for reimbursement through the APG "
        "fee schedule for CPT code 99457. Providers must bill using the correct "
        "revenue code and submit claims in accordance with UB-04 billing requirements. "
        "Claims must be submitted with value code 54 for newborn birth weight. "
        "Providers cannot bill CPT code 99457 more than one time per Medicaid member "
        "per 30-day period. Failure to comply will result in claim denial."
    )

    result = score_fragment(strong, "effective_date")
    confidence_scorer.OPENAI_API_KEY = original_key

    assert_true(result['semantic_skipped'], "semantic_skipped should be True")
    assert_true(
        result['structural_score'] >= 75,
        f"Test fragment should score >= 75 structurally, got {result['structural_score']}"
    )
    assert_true(
        result['tier'] != 'TIER_1',
        f"semantic_skipped=True must never produce TIER_1, got {result['tier']}"
    )
    assert_eq(result['tier'], 'TIER_2', "semantic_skipped high score must cap at TIER_2")
    print(f"  structural_score={result['structural_score']} | "
          f"tier={result['tier']} | semantic_skipped={result['semantic_skipped']}")
    print("  PASS")


# ---------------------------------------------------------------------------
# TEST 3 — fragment_writer: TIER_1 writes pending, confidence_score stored
# ---------------------------------------------------------------------------

def test_3_fragment_writer_tier1_pending_and_score():
    print("Test 3 — fragment_writer: TIER_1 → pending, confidence_score stored ...")

    assert_true(DB_PATH.exists(), f"DB not found at {DB_PATH}. Run db_init.py first.")

    unique_text = (
        "STEP1_VERIFY_FRAG: Effective July 1, 2025, providers must submit claims "
        "using revenue code 0450 for emergency room services billed under the "
        "APG fee schedule. Claims must be submitted with the correct type of bill "
        "and value code. Providers cannot bill CPT code 99457 more than once per "
        "member per 30-day period. Failure to comply will result in denial."
    )
    fake_sha = "v" * 64

    frag_id = write_fragment(
        fragment_text    = unique_text,
        page_number      = 99,
        source_filename  = "step1_verify_test.pdf",
        sha256           = fake_sha,
        extracted_date   = "2025-07-01",
        date_role        = "effective_date",
        structural_score = 90.0,
        semantic_score   = None,
        semantic_skipped = True,
        tier             = "TIER_1",
        confidence_score = 90.0,
    )

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM policy_fragments WHERE policy_fragment_id = ?",
        (frag_id,)
    ).fetchone()

    source_doc_id = row['source_document_id']

    assert_true(row is not None, "Inserted fragment not found in DB")
    assert_eq(float(row['confidence_score']), 90.0, "confidence_score")
    assert_eq(row['confidence_tier'],  'TIER_1',  "confidence_tier")
    assert_eq(row['review_status'],    'pending',
              "TIER_1 must write review_status='pending', not 'approved'")

    conn.execute(
        "DELETE FROM policy_fragments WHERE policy_fragment_id = ?", (frag_id,)
    )
    conn.execute(
        "DELETE FROM source_documents WHERE source_document_id = ?", (source_doc_id,)
    )
    conn.commit()
    conn.close()
    print("  PASS")


# ---------------------------------------------------------------------------
# TEST 4 — fragment_writer: TIER_3 writes pending under Admin-first governance
# ---------------------------------------------------------------------------

def test_4_tier3_writes_pending_admin_review():
    print("Test 4 — fragment_writer: TIER_3 → pending Admin review ...")

    unique_text = (
        "STEP1_VERIFY_TIER3: This is a low confidence test fragment with minimal "
        "policy signal. It references nothing specific about billing or claim submission."
    )
    fake_sha = "w" * 64

    frag_id = write_fragment(
        fragment_text    = unique_text,
        page_number      = 99,
        source_filename  = "step1_verify_tier3_test.pdf",
        sha256           = fake_sha,
        extracted_date   = None,
        date_role        = "none",
        structural_score = 10.0,
        semantic_score   = None,
        semantic_skipped = True,
        tier             = "TIER_3",
        confidence_score = 10.0,
    )

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM policy_fragments WHERE policy_fragment_id = ?",
        (frag_id,)
    ).fetchone()

    source_doc_id = row['source_document_id']

    assert_true(row is not None, "Inserted TIER_3 fragment not found in DB")
    assert_eq(row['confidence_tier'], 'TIER_3', "confidence_tier")
    assert_eq(
        row['review_status'],
        'pending',
        "TIER_3 must write review_status='pending' under Admin-first governance"
    )

    conn.execute(
        "DELETE FROM policy_fragments WHERE policy_fragment_id = ?", (frag_id,)
    )
    conn.execute(
        "DELETE FROM source_documents WHERE source_document_id = ?", (source_doc_id,)
    )
    conn.commit()
    conn.close()
    print("  PASS")


# ---------------------------------------------------------------------------
# TEST 5 — crawler_runs has correct column names
# ---------------------------------------------------------------------------

def test_5_crawler_runs_columns():
    print("Test 5 — crawler_runs column names match schema ...")

    conn  = sqlite3.connect(str(DB_PATH))
    cols  = {row[1] for row in conn.execute("PRAGMA table_info(crawler_runs)").fetchall()}
    conn.close()

    required = {'documents_found', 'documents_added', 'documents_skipped', 'documents_failed'}
    old_cols = {'docs_found', 'docs_downloaded', 'docs_skipped', 'docs_errored'}

    missing     = required - cols
    present_old = old_cols & cols

    assert_true(not missing,     f"crawler_runs missing columns: {missing}")
    assert_true(not present_old, f"crawler_runs still has old column names: {present_old}")
    print("  PASS")


# ---------------------------------------------------------------------------
# TEST 6 — .env file exists and API key is accessible
# ---------------------------------------------------------------------------

def test_6_env_file():
    print("Test 6 — .env file and OPENAI_API_KEY ...")

    env_path = PROJECT_ROOT / '.env'
    if not env_path.exists():
        print("  SKIP — .env file not found. Create it with OPENAI_API_KEY=your-key")
        return

    import os
    key = os.environ.get('OPENAI_API_KEY', '')
    if not key:
        print("  SKIP — .env exists but OPENAI_API_KEY is empty or not loaded")
        return

    assert_true(len(key) > 20, "OPENAI_API_KEY looks too short to be valid")
    print(f"  API key loaded: ...{key[-4:]}")
    print("  PASS")


# ---------------------------------------------------------------------------
# RUN ALL
# ---------------------------------------------------------------------------

def main():
    print()
    print("=" * 65)
    print("  DPKB STEP 1 VERIFICATION")
    print("=" * 65)

    tests = [
        test_1_thresholds_from_constants,
        test_2_semantic_skipped_cap,
        test_3_fragment_writer_tier1_pending_and_score,
        test_4_tier3_writes_pending_admin_review,
        test_5_crawler_runs_columns,
        test_6_env_file,
    ]

    passed = failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL — {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR — {type(e).__name__}: {e}")
            failed += 1

    print()
    print("=" * 65)
    if failed == 0:
        print(f"  ALL {passed} CHECKS PASSED")
    else:
        print(f"  {passed} passed, {failed} FAILED")
    print("=" * 65)
    print()

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()