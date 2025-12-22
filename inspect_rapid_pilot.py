
import duckdb
import os

ARTIFACT = "./Scout_Data_Artifacts/Western_Leads_2023_Pilot_Top5k.parquet"

def inspect():
    if not os.path.exists(ARTIFACT):
        print(f"Artifact not found: {ARTIFACT}")
        return

    print(f"Inspecting: {ARTIFACT}")
    print(f"Size: {os.path.getsize(ARTIFACT)/1024/1024:.2f} MB")
    
    con = duckdb.connect()
    # Check row count
    row_count = con.execute(f"SELECT COUNT(*) FROM '{ARTIFACT}'").fetchone()[0]
    print(f"Total Rows: {row_count}")
    
    # Check Verification: Brokers vs TPAs
    print("\n[BROKER ANALYSIS]")
    brokers = con.execute(f"SELECT BROKERS, COUNT(*) as cnt FROM '{ARTIFACT}' GROUP BY BROKERS ORDER BY cnt DESC LIMIT 5").fetchall()
    for b in brokers:
        print(b)
        
    print("\n[TPA ANALYSIS]")
    tpas = con.execute(f"SELECT TPAS, COUNT(*) as cnt FROM '{ARTIFACT}' GROUP BY TPAS ORDER BY cnt DESC LIMIT 5").fetchall()
    for t in tpas:
        print(t)

    # Check Source
    print("\n[SOURCE CHECK]")
    print(con.execute(f"SELECT DISTINCT SOURCE, RUN_DATE FROM '{ARTIFACT}'").fetchall())

if __name__ == "__main__":
    inspect()
