import sqlite3

conn = sqlite3.connect('db/dpkb.db')

print("FRAGMENTS BY TIER:")
rows = conn.execute("""
    SELECT confidence_tier, review_status, COUNT(*) as n
    FROM policy_fragments
    GROUP BY confidence_tier, review_status
    ORDER BY confidence_tier
""").fetchall()
for r in rows:
    print(f"  {r[0]} | {r[1]} | count: {r[2]}")

print("\nSAMPLE APPROVED FRAGMENTS:")
rows = conn.execute("""
    SELECT pf.confidence_tier,
           pf.extracted_effective_start_date,
           pf.structural_score,
           substr(pf.fragment_text_raw, 1, 150) as preview,
           sd.document_title
    FROM policy_fragments pf
    JOIN source_documents sd ON pf.source_document_id = sd.source_document_id
    WHERE pf.review_status = 'approved'
    LIMIT 3
""").fetchall()
for r in rows:
    print(f"\n  Tier: {r[0]} | Date: {r[1]} | Score: {r[2]}")
    print(f"  Source: {r[4]}")
    print(f"  Text: {r[3]}...")

print("\nSOURCE DOCUMENTS IN DB:")
rows = conn.execute("""
    SELECT document_title, doc_type, sha256_hash
    FROM source_documents
    ORDER BY source_document_id
""").fetchall()
for r in rows:
    print(f"  {r[0]} | {r[1]}")

conn.close()