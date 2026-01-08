import sqlite3

DB_PATH = "leads_pilot.db"

def inspect():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(c.fetchall())
    conn.close()

if __name__ == "__main__":
    inspect()
