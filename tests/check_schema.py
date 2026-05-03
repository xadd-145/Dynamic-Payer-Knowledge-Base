import sqlite3
conn = sqlite3.connect('db/dpkb.db')
sql = conn.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name='rule_evidence_links'"
).fetchone()
print(sql[0])
conn.close()