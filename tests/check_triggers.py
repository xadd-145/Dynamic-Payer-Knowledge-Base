import sqlite3
conn = sqlite3.connect('db/dpkb.db')
triggers = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'").fetchall()
print(f"Found {len(triggers)} triggers:")
for t in triggers:
    print(f"\nName: {t[0]}")
    print(f"SQL: {t[1]}")
conn.close()