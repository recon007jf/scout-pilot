import sqlite3

DB_PATH = "leads_pilot.db"

def inspect():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA table_info(leads_pilot)")
    rows = c.fetchall()
    for r in rows:
        print(r)
    conn.close()

if __name__ == "__main__":
    inspect()
