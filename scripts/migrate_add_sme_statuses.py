# scripts/migrate_add_sme_statuses.py
import sqlite3
from pathlib import Path

DB_PATH = Path("db/dpkb.db")
conn = sqlite3.connect(DB_PATH)

print("Reading existing table...")
pragma = conn.execute("PRAGMA table_info(policy_fragments)").fetchall()
cols = [d[1] for d in pragma]
rows = conn.execute("SELECT * FROM policy_fragments").fetchall()
print(f"  {len(rows)} rows")

conn.execute("DROP TABLE IF EXISTS policy_fragments_old")
conn.execute("ALTER TABLE policy_fragments RENAME TO policy_fragments_old")

conn.execute("""
CREATE TABLE policy_fragments (
    policy_fragment_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_document_id             INTEGER NOT NULL REFERENCES source_documents(source_document_id),
    fragment_text_raw              TEXT    NOT NULL,
    fragment_sequence_number       INTEGER,
    page_number_start              INTEGER,
    page_number_end                INTEGER,
    section_reference              TEXT,
    extracted_effective_start_date TEXT,
    extracted_effective_end_date   TEXT,
    date_role_label                TEXT,
    detected_anchor_type_code      TEXT,
    detected_anchor_value          TEXT,
    confidence_score               REAL    NOT NULL DEFAULT 0.0,
    confidence_tier                TEXT    NOT NULL DEFAULT 'TIER_3',
    structural_score               REAL,
    semantic_score                 REAL,
    semantic_skipped               INTEGER NOT NULL DEFAULT 0 CHECK (semantic_skipped IN (0, 1)),
    review_status                  TEXT    NOT NULL DEFAULT 'pending',
    reviewed_by                    TEXT,
    reviewed_at                    TEXT,
    review_notes                   TEXT,
    created_at                     TEXT    NOT NULL,
    CHECK (confidence_tier IN ('TIER_1','TIER_2','TIER_3')),
    CHECK (review_status IN (
        'pending','approved','rejected','escalated',
        'sme_approved','sme_rejected'
    ))
)
""")

col_names = ", ".join(cols)
placeholders = ", ".join(["?" for _ in cols])
for row in rows:
    conn.execute(
        f"INSERT INTO policy_fragments ({col_names}) VALUES ({placeholders})",
        list(row)
    )

conn.execute("DROP TABLE policy_fragments_old")
conn.commit()

verify = conn.execute("SELECT COUNT(*) FROM policy_fragments").fetchone()[0]
print(f"Verification: {verify} rows")

dist = conn.execute("""
    SELECT confidence_tier, review_status, COUNT(*)
    FROM policy_fragments
    GROUP BY confidence_tier, review_status
    ORDER BY confidence_tier, review_status
""").fetchall()
for r in dist:
    print(f"  {r[0]} | {r[1]} | {r[2]}")

# Test the new constraint works
try:
    conn.execute("""
        UPDATE policy_fragments SET review_status='sme_approved'
        WHERE policy_fragment_id=999999
    """)
    conn.rollback()
    print("CONSTRAINT TEST: sme_approved is now allowed")
except Exception as e:
    print(f"CONSTRAINT TEST FAILED: {e}")

conn.close()
print("Done.")