import sqlite3
from pathlib import Path

conn = sqlite3.connect('db/dpkb.db')

print("Reading existing rule_evidence_links...")
pragma = conn.execute("PRAGMA table_info(rule_evidence_links)").fetchall()
cols = [d[1] for d in pragma]
rows = conn.execute("SELECT * FROM rule_evidence_links").fetchall()
print(f"  {len(rows)} rows, columns: {cols}")

conn.execute("DROP TABLE IF EXISTS rule_evidence_links_old")
conn.execute("ALTER TABLE rule_evidence_links RENAME TO rule_evidence_links_old")

conn.execute("""
CREATE TABLE rule_evidence_links (
    rule_evidence_link_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_version_id        INTEGER NOT NULL REFERENCES rule_versions(rule_version_id),
    source_document_id     INTEGER NOT NULL REFERENCES source_documents(source_document_id),
    policy_fragment_id     INTEGER REFERENCES policy_fragments(policy_fragment_id),
    page_number_start      INTEGER,
    page_number_end        INTEGER,
    section_reference      TEXT,
    citation_text          TEXT,
    created_at             TEXT    NOT NULL
)
""")

col_names = ", ".join(cols)
placeholders = ", ".join(["?" for _ in cols])
for row in rows:
    conn.execute(
        f"INSERT INTO rule_evidence_links ({col_names}) VALUES ({placeholders})",
        list(row)
    )

conn.execute("DROP TABLE rule_evidence_links_old")
conn.commit()

verify = conn.execute("SELECT COUNT(*) FROM rule_evidence_links").fetchone()[0]
print(f"Restored: {verify} rows")

sql = conn.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name='rule_evidence_links'"
).fetchone()[0]
print("New schema:")
print(sql)
conn.close()
print("Done.")