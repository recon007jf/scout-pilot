
import duckdb
import os

ARTIFACT = "./Scout_Data_Artifacts/Western_Leads_2023_Platinum.parquet"

def inspect():
    if not os.path.exists(ARTIFACT):
        print(f"Artifact not found: {ARTIFACT}")
        return

    print(f"Inspecting: {ARTIFACT}")
    print(f"Size: {os.path.getsize(ARTIFACT)/1024/1024:.2f} MB")
    
    con = duckdb.connect()
    con.execute(f"CREATE VIEW leads AS SELECT * FROM read_parquet('{ARTIFACT}')")
    
    row_count = con.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    print(f"Total Rows: {row_count}")
    
    brokers = con.execute("SELECT COUNT(*) FROM leads WHERE BROKERS != 'Unknown'").fetchone()[0]
    print(f"With Brokers: {brokers} ({brokers/row_count*100:.1f}%)")
    
    tpas = con.execute("SELECT COUNT(*) FROM leads WHERE TPAS != 'None Listed'").fetchone()[0]
    print(f"With TPAS: {tpas} ({tpas/row_count*100:.1f}%)")
    
    print("\nSAMPLE:")
    print(con.execute("SELECT EMPLOYER_NAME, BROKERS, TPAS FROM leads LIMIT 3").fetchall())

if __name__ == "__main__":
    inspect()
