# modules/module_b/fragment_writer.py
"""
Module B — Step 4: Fragment Writer

Writes scored fragments to policy_fragments table.

GOVERNANCE RULE:
  Nothing is auto-approved here.
  TIER_1 = high-confidence candidate for Admin review (pending).
  TIER_2 = Admin/Arjav review (pending).
  TIER_3 = weak/ambiguous candidate for Admin review (pending).
  Arjav/Admin decides what becomes an authoritative rule.

review_status mapping:
  TIER_1 → 'pending'   (Admin queue, shown at top by confidence score)
  TIER_2 → 'pending'   (Admin queue, standard order)
  TIER_3 → 'pending'   (Admin queue, lower-confidence candidate)
"""

import sqlite3
import sys
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# PROJECT ROOT
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / '.env')

from db_init import utc_now

DB_PATH = Path(__file__).resolve().parents[2] / 'db' / 'dpkb.db'


# ---------------------------------------------------------------------------
# DB CONNECTION — 3 PRAGMAs on every connection
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA busy_timeout = 5000')
    return conn


# ---------------------------------------------------------------------------
# GET OR CREATE SOURCE DOCUMENT
# ---------------------------------------------------------------------------

def _get_or_create_source_doc(conn, source_filename: str, sha256: str) -> int:
    row = conn.execute(
        'SELECT source_document_id FROM source_documents WHERE sha256_hash = ?',
        (sha256,)
    ).fetchone()

    if row:
        return row['source_document_id']

    if 'speced' in source_filename.lower():
        doc_type = 'BULLETIN_SPECIAL'
    elif 'mu_no' in source_filename.lower():
        doc_type = 'BULLETIN'
    else:
        doc_type = 'MANUAL'

    source_url = (
        'https://www.health.ny.gov/health_care/medicaid/program/update/'
        + source_filename
    )
    now = utc_now()

    conn.execute("""
        INSERT INTO source_documents
            (source_type_id, doc_type, document_title,
             file_path_local, source_url, sha256_hash,
             retrieved_at, is_current, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (1, doc_type, source_filename, source_filename,
          source_url, sha256, now, 1, now))

    conn.commit()

    return conn.execute(
        'SELECT source_document_id FROM source_documents WHERE sha256_hash = ?',
        (sha256,)
    ).fetchone()['source_document_id']


# ---------------------------------------------------------------------------
# TIER → REVIEW STATUS MAPPING
# ---------------------------------------------------------------------------

def _review_status_for_tier(tier: str) -> str:
    """
    Governance rule:
      All AI-extracted fragments go to Admin/Arjav first.

    Confidence tier is only a priority signal:
      TIER_1 = strongest candidate
      TIER_2 = standard review candidate
      TIER_3 = weak/ambiguous candidate

    TIER_3 does NOT automatically go to SME.
    Admin must manually click "Escalate to SME" for SME visibility.
    """
    if tier in ('TIER_1', 'TIER_2', 'TIER_3'):
        return 'pending'

    raise ValueError(f"Unknown confidence tier: {tier}")


# ---------------------------------------------------------------------------
# WRITE FRAGMENT TO DB
# ---------------------------------------------------------------------------

def write_fragment(
    fragment_text:    str,
    page_number:      int,
    source_filename:  str,
    sha256:           str,
    extracted_date:   str | None,
    date_role:        str,
    structural_score: float,
    semantic_score:   float | None,
    semantic_skipped: bool,
    tier:             str,
    confidence_score: float | None = None,
) -> int:
    """
    Write one scored fragment to policy_fragments table.
    Returns policy_fragment_id.

    confidence_score should be final_score from score_fragment().
    Falls back to structural_score if not provided.
    """
    conn = _get_conn()

    try:
        source_doc_id = _get_or_create_source_doc(conn, source_filename, sha256)

        # Deduplication — skip if same text + same source already exists
        existing = conn.execute("""
            SELECT policy_fragment_id FROM policy_fragments
            WHERE source_document_id = ? AND fragment_text_raw = ?
        """, (source_doc_id, fragment_text)).fetchone()

        if existing:
            return existing['policy_fragment_id']

        if confidence_score is None:
            confidence_score = structural_score

        review_status = _review_status_for_tier(tier)
        now           = utc_now()

        conn.execute("""
            INSERT INTO policy_fragments
                (source_document_id,
                 fragment_text_raw,
                 page_number_start,
                 extracted_effective_start_date,
                 date_role_label,
                 confidence_score,
                 structural_score,
                 semantic_score,
                 semantic_skipped,
                 confidence_tier,
                 review_status,
                 created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_doc_id,
            fragment_text,
            page_number,
            extracted_date,
            date_role,
            confidence_score,
            structural_score,
            semantic_score,
            1 if semantic_skipped else 0,
            tier,
            review_status,
            now,
        ))

        conn.commit()
        fragment_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        return fragment_id

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# STANDALONE TEST
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    from modules.module_b.extractor import extract_fragments
    from modules.module_b.date_classifier import classify_dates
    from modules.module_b.confidence_scorer import score_fragment

    data_dir = PROJECT_ROOT / 'data' / 'raw'
    pdfs     = sorted(data_dir.glob('*_pr.pdf'))

    if not pdfs:
        print('No _pr.pdf files found in data/raw/')
        sys.exit(1)

    written = {'TIER_1': 0, 'TIER_2': 0, 'TIER_3': 0}
    errors  = 0

    for pdf in pdfs[:2]:
        print(f'\nProcessing: {pdf.name}')
        result = extract_fragments(str(pdf))

        for frag in result['fragments']:
            try:
                date_result = classify_dates(
                    frag['fragment_text'],
                    frag.get('inherited_date'),
                )
                score_result = score_fragment(
                    frag['fragment_text'],
                    date_result['date_role'],
                    date_result.get('date_source', 'direct'),
                )

                write_fragment(
                    fragment_text    = frag['fragment_text'],
                    page_number      = frag['page_number'],
                    source_filename  = result['source_filename'],
                    sha256           = result['sha256_hash'],
                    extracted_date   = date_result['extracted_date'],
                    date_role        = date_result['date_role'],
                    structural_score = score_result['structural_score'],
                    semantic_score   = score_result['semantic_score'],
                    semantic_skipped = score_result['semantic_skipped'],
                    tier             = score_result['tier'],
                    confidence_score = score_result['final_score'],
                )

                written[score_result['tier']] += 1

            except Exception as e:
                print(f'  Error: {e}')
                errors += 1

    print(f'\nWRITE RESULTS:')
    print(f'  TIER_1 written : {written["TIER_1"]}')
    print(f'  TIER_2 written : {written["TIER_2"]}')
    print(f'  TIER_3 written : {written["TIER_3"]}')
    print(f'  Errors         : {errors}')

    conn = sqlite3.connect(str(DB_PATH))
    count = conn.execute('SELECT COUNT(*) FROM policy_fragments').fetchone()[0]
    conn.close()
    print(f'\n  Total fragments in DB: {count}')