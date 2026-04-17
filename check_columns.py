from db_init import get_connection
conn = get_connection()
print("rule_versions columns:")
print([r[1] for r in conn.execute("PRAGMA table_info(rule_versions)").fetchall()])
print()
print("atomic_rules columns:")
print([r[1] for r in conn.execute("PRAGMA table_info(atomic_rules)").fetchall()])
conn.close()