
import duckdb
import os

ARTIFACT = "./Scout_Data_Artifacts/Western_Broker_Map_2023.parquet"

def inspect():
    if not os.path.exists(ARTIFACT):
        print(f"Artifact not found: {ARTIFACT}")
        return

    print(f"Inspecting: {ARTIFACT}")
    print(f"Size: {os.path.getsize(ARTIFACT)/1024/1024:.2f} MB")
    
    try:
        con = duckdb.connect()
        # Row Count
        row_count = con.execute(f"SELECT COUNT(*) FROM '{ARTIFACT}'").fetchone()[0]
        print(f"Total Client Rows: {row_count}")
        
        # Unique Brokers
        broker_count = con.execute(f"SELECT COUNT(DISTINCT TARGET_BROKER_FIRM) FROM '{ARTIFACT}'").fetchone()[0]
        print(f"Unique Broker Firms: {broker_count}")

        # Top Brokers
        print("\n[TOP 5 BROKER FIRMS BY CLIENT VOLUME]")
        top = con.execute(f"SELECT TARGET_BROKER_FIRM, COUNT(*) as Clients, SUM(CLIENT_LIVES) as Lives FROM '{ARTIFACT}' GROUP BY TARGET_BROKER_FIRM ORDER BY Clients DESC LIMIT 5").fetchall()
        for t in top:
            print(t)

        # Sample Row
        print("\n[SAMPLE ROW]")
        print(con.execute(f"SELECT * FROM '{ARTIFACT}' LIMIT 1").fetchall())
        
    except Exception as e:
        print(f"Inspection Failed: {e}")

if __name__ == "__main__":
    inspect()
