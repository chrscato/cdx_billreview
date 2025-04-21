import sqlite3

conn = sqlite3.connect("filemaker.db")
cursor = conn.cursor()

# Show all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

# Optionally: show schema for each table
for table in tables:
    print(f"\nSchema for {table[0]}:")
    cursor.execute(f"PRAGMA table_info({table[0]});")
    for col in cursor.fetchall():
        print(col)
