import sqlite3
conn = sqlite3.connect('db/dpkb.db')
cols = [r[1] for r in conn.execute("PRAGMA table_info(source_documents)")]
print('source_documents columns:')
print(cols)
conn.close()