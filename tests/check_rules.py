import sqlite3
conn = sqlite3.connect('db/dpkb.db')
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT
        ar.rule_code,
        ar.rule_topic_id,
        rt.topic_code,
        rv.rule_version_id,
        rv.effective_start_date,
        rv.effective_end_date,
        rv.lifecycle_status,
        rv.is_published
    FROM rule_versions rv
    JOIN atomic_rules ar ON ar.atomic_rule_id = rv.atomic_rule_id
    JOIN rule_topics rt ON rt.rule_topic_id = ar.rule_topic_id
    WHERE ar.rule_code IN ('OUTPT-REPRO-001', 'OUTPT-RPM-002')
""").fetchall()

print(f"Found {len(rows)} rule versions:")
for r in rows:
    print(dict(r))

# Also check what topic_id Outpatient Surgery maps to
topics = conn.execute(
    "SELECT rule_topic_id, topic_code, topic_name FROM rule_topics"
).fetchall()
print("\nAll topics:")
for t in topics:
    print(dict(t))

conn.close()