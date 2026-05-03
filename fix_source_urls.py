import sqlite3
import re
from pathlib import Path

conn = sqlite3.connect('db/dpkb.db')
rows = conn.execute(
    "SELECT source_document_id, document_title, source_url, file_path_local FROM source_documents"
).fetchall()

fixed = 0
for row in rows:
    doc_id, title, url, filepath = row
    if not filepath:
        continue

    filename = Path(filepath).name
    # Extract 2-digit year from filename e.g. mu_no16_nov23_pr.pdf -> 23 -> 2023
    match = re.search(r'_([a-z]{3})(\d{2})_', filename)
    if not match:
        continue

    year_2digit = match.group(2)
    year_4digit = "20" + year_2digit

    correct_url = (
        f"https://www.health.ny.gov/health_care/medicaid/program/update"
        f"/{year_4digit}/docs/{filename}"
    )

    if url != correct_url:
        conn.execute(
            "UPDATE source_documents SET source_url = ? WHERE source_document_id = ?",
            (correct_url, doc_id)
        )
        print(f"Fixed doc {doc_id}: {filename} -> {correct_url}")
        fixed += 1

conn.commit()
print(f"\nTotal fixed: {fixed}")

# Verify a few
samples = conn.execute(
    "SELECT document_title, source_url FROM source_documents LIMIT 5"
).fetchall()
for s in samples:
    print(s)

conn.close()