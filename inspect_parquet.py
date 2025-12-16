
import duckdb

PATH = "backend/Scout_Data_Artifacts/Western_Broker_Map_2023.parquet"

con = duckdb.connect()
try:
    print(f"Inspecting: {PATH}")
    con.execute(f"CREATE VIEW v AS SELECT * FROM '{PATH}'")
    count = con.execute("SELECT COUNT(*) FROM v").fetchone()[0]
    print(f"Row Count: {count}")
    
    if count > 0:
        print("\nColumns:")
        print([c[0] for c in con.execute("DESCRIBE v").fetchall()])
        
        print("\nSample Data (First 3 rows):")
        res = con.execute("SELECT * FROM v LIMIT 3").fetchall()
        for r in res:
            print(r)
            
        print("\nNull Checks:")
        print(f"Null Firm: {con.execute('SELECT COUNT(*) FROM v WHERE TARGET_BROKER_FIRM IS NULL').fetchone()[0]}")
        print(f"Null City: {con.execute('SELECT COUNT(*) FROM v WHERE TARGET_BROKER_CITY IS NULL').fetchone()[0]}")
        print(f"Null State: {con.execute('SELECT COUNT(*) FROM v WHERE TARGET_BROKER_STATE IS NULL').fetchone()[0]}")
            
except Exception as e:
    print(f"Error: {e}")
