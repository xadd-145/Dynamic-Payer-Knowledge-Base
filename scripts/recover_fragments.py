# scripts/recover_fragments.py
import sqlite3
from pathlib import Path

DB_PATH = Path("db/dpkb.db")
conn = sqlite3.connect(DB_PATH)

# Check what tables exist
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tables:", [t[0] for t in tables])

# Check old table row count
old_count = conn.execute("SELECT COUNT(*) FROM policy_fragments_old").fetchone()[0]
print(f"Rows in policy_fragments_old: {old_count}")

# Get columns from old table
cols = [d[1] for d in conn.execute("PRAGMA table_info(policy_fragments_old)").fetchall()]
print(f"Columns: {cols}")

# Drop the empty new table
conn.execute("DROP TABLE IF EXISTS policy_fragments")

# Rename old table back
conn.execute("ALTER TABLE policy_fragments_old RENAME TO policy_fragments")
conn.commit()

verify = conn.execute("SELECT COUNT(*) FROM policy_fragments").fetchone()[0]
print(f"Restored: {verify} rows in policy_fragments")
conn.close()
print("Recovery complete.")